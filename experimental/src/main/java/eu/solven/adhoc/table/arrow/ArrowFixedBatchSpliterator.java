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
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordFactory;
import eu.solven.adhoc.engine.cancel.CancelledQueryException;
import eu.solven.adhoc.engine.context.QueryPod;

/**
 * A fixed-range {@link Spliterator} over a single Arrow batch that was split off from an {@link ArrowBatchSpliterator}.
 *
 * <p>
 * The spliterator holds a reference to a sliced {@code VectorSchemaRoot} produced by {@link ArrowReflection#SLICE}.
 * Arrow's reference-counted allocator keeps the underlying {@code ArrowBuf} alive until the slice is closed, even after
 * the originating reader loads the next batch. {@link #closeRoot()} is therefore called automatically when the row
 * range is exhausted.
 *
 * <p>
 * The spliterator can itself be split (via {@link #trySplit()}) into smaller fixed-range sub-spliterators by slicing
 * the already-sliced root further, enabling fine-grained intra-batch parallelism.
 *
 * @author Benoit Lacelle
 */
final class ArrowFixedBatchSpliterator implements Spliterator<ITabularRecord> {
	private final Object slicedRoot;
	private final List<?> vectors;
	private final int rowCount;
	private final ITabularRecordFactory factory;
	private final QueryPod queryPod;
	private final int minSplitRows;

	private int currentRow;
	private boolean rootClosed;

	ArrowFixedBatchSpliterator(Object slicedRoot,
			int rowCount,
			ITabularRecordFactory factory,
			QueryPod queryPod,
			int minSplitRows) {
		this.slicedRoot = slicedRoot;
		this.rowCount = rowCount;
		this.factory = factory;
		this.queryPod = queryPod;
		this.minSplitRows = minSplitRows;
		try {
			this.vectors = (List<?>) ArrowReflection.GET_FIELD_VECTORS.invoke(slicedRoot);
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error getting field vectors from sliced Arrow root", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}

	@Override
	public boolean tryAdvance(Consumer<? super ITabularRecord> action) {
		if (currentRow >= rowCount) {
			closeRoot();
			return false;
		}

		if (queryPod.isCancelled()) {
			throw new CancelledQueryException("Query cancelled during Arrow stream iteration");
		}

		action.accept(ArrowReflection.buildRecord(vectors, currentRow, factory));
		currentRow++;

		if (currentRow >= rowCount) {
			closeRoot();
		}
		return true;
	}

	@Override
	public Spliterator<ITabularRecord> trySplit() {
		int remaining = rowCount - currentRow;
		if (remaining < minSplitRows) {
			return null;
		}
		int splitSize = remaining / 2;

		// Slice the already-sliced root: each level of slicing increments the ArrowBuf ref-count.
		try {
			Object subRoot = ArrowReflection.SLICE.invoke(slicedRoot, currentRow, splitSize);
			currentRow += splitSize;
			return new ArrowFixedBatchSpliterator(subRoot, splitSize, factory, queryPod, minSplitRows);
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error slicing Arrow VectorSchemaRoot", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}

	private void closeRoot() {
		if (!rootClosed) {
			rootClosed = true;
			ArrowReflection.closeRoot(slicedRoot);
		}
	}

	@Override
	public long estimateSize() {
		return rowCount - currentRow;
	}

	@Override
	public int characteristics() {
		// SIZED + SUBSIZED: exact row count known, enabling balanced parallel work distribution.
		// ORDERED: rows within a batch have a defined encounter order.
		// NONNULL: buildRecord never returns null.
		return ORDERED | NONNULL | SIZED | SUBSIZED;
	}

}
