/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.table.arrow;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.engine.cancel.CancelledQueryException;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.RequiredArgsConstructor;

/**
 * A {@link Spliterator} that lazily loads Arrow record batches from an {@code ArrowReader} and iterates their rows.
 *
 * <p>
 * <b>Sequential mode</b> (stream created with {@code parallel=false}): {@link #trySplit()} is never called by the
 * framework. Batch loading and row iteration both happen on the calling thread with no concurrency overhead.
 *
 * <p>
 * <b>Concurrent mode</b> (stream created with {@code parallel=true}): the framework calls {@link #trySplit()} to divide
 * work. Each call joins the head of the pre-fetch queue and returns an {@link ArrowFixedBatchSpliterator} (all data
 * already in memory, pure CPU, safe for FJP). Up to {@code prefetchCount} batch-loading futures are kept in flight on
 * virtual threads from {@link AdhocUnsafe#adhocMixedPool}; each future is chained from the previous via
 * {@link CompletableFuture#thenApplyAsync}, so the Arrow reader is accessed by at most one VT at a time.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public final class ArrowBatchSpliterator implements Spliterator<ITabularRecord> {

	final Object arrowReader;
	/** Shared immutable context forwarded to every {@link ArrowFixedBatchSpliterator} produced by this spliterator. */
	final ArrowBatchContext context;
	/**
	 * Maximum number of batch-loading futures held in the pre-fetch queue at any time. Only relevant in parallel mode
	 * (when {@link #trySplit()} is called); sequential mode ({@link #tryAdvance()} only) is unaffected.
	 */
	final int prefetchCount;

	/** Mutable cursor into the Arrow reader; updated by {@link #loadNextBatch()} and {@link #takeBatch()}. */
	final ArrowReaderCursor cursor = new ArrowReaderCursor();

	/**
	 * Queue of pre-fetched batch futures. Each entry is chained from the previous so that the Arrow reader is never
	 * accessed concurrently. Only populated in parallel mode when {@link #trySplit()} is called.
	 */
	private final Deque<CompletableFuture<Optional<Spliterator<ITabularRecord>>>> prefetchQueue = new ArrayDeque<>();

	/**
	 * Tail of the submission chain. Every new future must be chained from this to preserve sequential Arrow reader
	 * access across VT tasks.
	 */
	private CompletableFuture<?> lastSubmitted = CompletableFuture.completedFuture(null);

	@Override
	public boolean tryAdvance(Consumer<? super ITabularRecord> action) {
		// Initially, both variables are 0
		while (cursor.rowIndex >= cursor.batchSize) {
			// current batch is depleted: load another one
			if (!loadNextBatch()) {
				// no next batch: the stream is depleted
				return false;
			}
		}

		if (context.queryPod().isCancelled()) {
			throw new CancelledQueryException("Query cancelled during Arrow stream iteration");
		}

		// process current row
		action.accept(ArrowReflection.buildRecord(cursor.vectors, cursor.rowIndex, context.factory()));
		cursor.rowIndex++;

		return true;
	}

	private boolean loadNextBatch() {
		if (cursor.exhausted) {
			return false;
		}
		try {
			boolean hasMore = (boolean) ArrowReflection.LOAD_NEXT_BATCH.invoke(arrowReader);
			if (!hasMore) {
				cursor.exhausted = true;
				return false;
			}
			Object root = ArrowReflection.GET_VECTOR_SCHEMA_ROOT.invoke(arrowReader);
			cursor.root = root;
			cursor.batchSize = (int) ArrowReflection.GET_ROW_COUNT.invoke(root);
			cursor.vectors = (List<?>) ArrowReflection.GET_FIELD_VECTORS.invoke(root);
			cursor.rowIndex = 0;
			return true;
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error loading next Arrow batch", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}

	/**
	 * Takes the current Arrow batch as a fixed-size {@link Spliterator}, advancing this spliterator past the whole
	 * batch. Returns empty when no more batches are available.
	 *
	 * <p>
	 * Only called from VT tasks submitted via {@link #fillPrefetchQueue()}, which chains them sequentially so the Arrow
	 * reader is never accessed concurrently.
	 */
	Optional<Spliterator<ITabularRecord>> takeBatch() {
		if (cursor.exhausted) {
			return Optional.empty();
		}
		if (cursor.rowIndex >= cursor.batchSize && !loadNextBatch()) {
			return Optional.empty();
		}
		int remaining = cursor.batchSize - cursor.rowIndex;
		try {
			Object slicedRoot = ArrowReflection.SLICE.invoke(cursor.root, cursor.rowIndex, remaining);
			// BEWARE This is equivalent with cursor.rowIndex = cursor.batchSize
			cursor.rowIndex += remaining;
			return Optional.of(new ArrowFixedBatchSpliterator(slicedRoot, remaining, context));
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error slicing Arrow VectorSchemaRoot", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}

	/**
	 * Submits batch-loading futures to {@link AdhocUnsafe#adhocMixedPool} until the queue holds {@code prefetchCount}
	 * entries or the reader is known to be exhausted. Each future is chained from {@code lastSubmitted} so that
	 * consecutive VT tasks access the Arrow reader one at a time.
	 */
	private void fillPrefetchQueue() {
		while (prefetchQueue.size() < prefetchCount && !cursor.exhausted) {
			CompletableFuture<Optional<Spliterator<ITabularRecord>>> next =
					lastSubmitted.thenApplyAsync(ignored -> takeBatch(), AdhocUnsafe.adhocMixedPool);
			prefetchQueue.addLast(next);
			// Advance the chain tail so the next submission waits for this one to complete
			lastSubmitted = next.thenAccept(ignored -> {
			});
		}
	}

	/**
	 * Splits off a pre-fetched batch for parallel processing.
	 *
	 * <p>
	 * On the first call the queue is empty, so {@link #fillPrefetchQueue()} submits up to {@code prefetchCount} futures
	 * before joining the head. On subsequent calls the head is typically already resolved (IO overlapped with FJP
	 * processing of the previously returned batch), and a replacement future is appended to keep the queue full.
	 *
	 * <p>
	 * Returns {@code null} when no further batches are available, signalling the framework to stop splitting.
	 */
	@Override
	public Spliterator<ITabularRecord> trySplit() {
		fillPrefetchQueue();

		if (prefetchQueue.isEmpty()) {
			return null;
		}

		// Join the head future. It was submitted before FJP started processing the previous batch,
		// so it is typically already resolved by the time we get here.
		Optional<Spliterator<ITabularRecord>> batch = prefetchQueue.pollFirst().join();

		// Refill: keep up to prefetchCount futures in flight so the next trySplit() finds a ready batch
		fillPrefetchQueue();

		return batch.orElse(null);
	}

	@Override
	public long estimateSize() {
		// Arrow does not enable know the number of rows in advance
		return Long.MAX_VALUE;
	}

	@Override
	public int characteristics() {
		return ORDERED | NONNULL;
	}
}
