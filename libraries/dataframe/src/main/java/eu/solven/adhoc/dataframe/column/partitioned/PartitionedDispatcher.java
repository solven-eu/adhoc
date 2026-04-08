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
package eu.solven.adhoc.dataframe.column.partitioned;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import eu.solven.adhoc.stream.IConsumingStream;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages the shared state between producer and consumer sides of a partitioned dispatch. The producer batches elements
 * by partition index and pushes them into bounded queues; consumer threads drain their respective queues sequentially.
 *
 * <p>
 * Lifecycle: {@link #startConsumers()}, then {@link #produce(IConsumingStream)}, then {@link #awaitAndRethrow()}.
 *
 * @param <T>
 *            the element type
 * @author Benoit Lacelle
 */
@Slf4j
class PartitionedDispatcher<T> {
	private final int nbPartitions;
	private final int batchSize;
	private final ToIntFunction<T> partitioner;
	private final Consumer<T> consumer;
	private final Executor executor;

	private final AtomicReference<Throwable> firstError = new AtomicReference<>();
	private final CountDownLatch latch;
	private final BlockingDeque<List<Object>>[] queues;

	/** Empty list sentinel signals end-of-stream to consumer threads. */
	private static final List<Object> POISON = List.of();

	/**
	 * Creates a dispatcher from the given parameters.
	 */
	@SuppressWarnings("unchecked")
	public PartitionedDispatcher(PartitionedForEachParameters<T> parameters) {
		this.nbPartitions = parameters.getNbPartitions();
		this.batchSize = parameters.getBatchSize();
		this.partitioner = parameters.getPartitioner();
		this.consumer = parameters.getConsumer();
		this.executor = parameters.getExecutor();
		this.latch = new CountDownLatch(nbPartitions);

		int queueCapacity = parameters.getQueueCapacity();
		this.queues = new BlockingDeque[nbPartitions];
		for (int i = 0; i < nbPartitions; i++) {
			queues[i] = new LinkedBlockingDeque<>(queueCapacity);
		}
	}

	/**
	 * Starts one consumer thread per partition. Each thread drains batches from its queue and applies the consumer to
	 * every element.
	 */
	@SuppressWarnings("unchecked")
	public void startConsumers() {
		for (int i = 0; i < nbPartitions; i++) {
			BlockingDeque<List<Object>> queue = queues[i];
			executor.execute(() -> {
				try {
					while (true) {
						List<Object> batch = queue.take();
						if (batch.isEmpty()) {
							// Poison pill — end of stream
							break;
						}
						for (Object item : batch) {
							consumer.accept((T) item);
						}
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					firstError.compareAndSet(null, e);
				} catch (RuntimeException e) {
					firstError.compareAndSet(null, e);
				} finally {
					latch.countDown();
				}
			});
		}
	}

	/**
	 * Drives iteration on the current thread, batching elements by partition index before dispatching to queues. After
	 * iteration completes (or fails), sends poison pills to all consumer threads.
	 */
	@SuppressWarnings("unchecked")
	public void produce(IConsumingStream<T> stream) {
		List<Object>[] buffers = new List[nbPartitions];
		for (int i = 0; i < nbPartitions; i++) {
			buffers[i] = new ArrayList<>(batchSize);
		}

		try {
			stream.forEach(element -> {
				if (firstError.get() != null) {
					return;
				}
				int idx = partitioner.applyAsInt(element);
				buffers[idx].add(element);
				if (buffers[idx].size() == batchSize) {
					flushBuffer(queues[idx], buffers[idx]);
					buffers[idx] = new ArrayList<>(batchSize);
				}
			});
			// Flush remaining partial batches
			for (int i = 0; i < nbPartitions; i++) {
				if (!buffers[i].isEmpty()) {
					flushBuffer(queues[i], buffers[i]);
				}
			}
		} catch (RuntimeException e) {
			firstError.compareAndSet(null, e);
		} finally {
			sendPoison();
		}
	}

	/**
	 * Waits for all consumer threads to finish, then rethrows the first error (if any).
	 */
	public void awaitAndRethrow() {
		try {
			latch.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			firstError.compareAndSet(null, e);
		}

		Throwable error = firstError.get();
		if (error instanceof RuntimeException re) {
			throw re;
		} else if (error != null) {
			throw new IllegalStateException("Partition consumer failed", error);
		}
	}

	/**
	 * Sends poison pills to all consumer threads. On error, clears pending batches first so consumers stop quickly.
	 */
	protected void sendPoison() {
		boolean hasError = firstError.get() != null;
		for (int i = 0; i < nbPartitions; i++) {
			BlockingDeque<List<Object>> queue = queues[i];
			try {
				if (hasError) {
					int discarded = queue.size();
					if (discarded > 0) {
						log.warn("Discarding {} pending batches for partitionIndex={}", discarded, i);
					}
					queue.clear();
					queue.putFirst(POISON);
				} else {
					// Normal completion: append poison after all data batches
					queue.put(POISON);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				firstError.compareAndSet(null, e);
			}
		}
	}

	/**
	 * Puts a batch into the queue, applying backpressure if full.
	 */
	protected void flushBuffer(BlockingDeque<List<Object>> queue, List<Object> batch) {
		try {
			queue.put(batch);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			firstError.compareAndSet(null, e);
		}
	}
}
