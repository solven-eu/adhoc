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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility methods for hash-based partitioning as used by {@link IPartitioned} implementations.
 *
 * @author Benoit Lacelle
 */
@Slf4j
@UtilityClass
public class PartitioningHelpers {

	/**
	 * Maps {@code key} to a partition index in {@code [0, nbPartitions)}.
	 *
	 * <p>
	 * {@link Math#floorMod} is used rather than {@code %} so that negative hash codes — including
	 * {@link Integer#MIN_VALUE} — always produce a non-negative index.
	 *
	 * @param key
	 *            the key to route; must not be {@code null}
	 * @param nbPartitions
	 *            the total number of partitions; must be strictly positive
	 * @return a value in {@code [0, nbPartitions)}
	 */
	public static int getPartitionIndex(Object key, int nbPartitions) {
		return Math.floorMod(key.hashCode(), nbPartitions);
	}

	/**
	 * Partition-aware terminal operation: each element from the source stream is routed by the partitioner to one of N
	 * dedicated consumer threads, so that all elements sharing the same partition index are processed sequentially by
	 * the same thread. This eliminates write contention across partitions.
	 *
	 * <p>
	 * This is conceptually similar to {@link java.util.stream.Stream#parallel()}, but deterministic: the partition
	 * assignment is controlled by the caller, and each partition's elements are consumed in encounter order.
	 *
	 * @param <T>
	 *            the element type
	 * @param parameters
	 *            all configuration for the partitioned consumption
	 */
	@SuppressWarnings("unchecked")
	public static <T> void forEachPartitioned(PartitionedForEachParameters<T> parameters) {
		int nbPartitions = parameters.getNbPartitions();
		int batchSize = parameters.getBatchSize();

		AtomicReference<Throwable> firstError = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(nbPartitions);

		// Queues carry List batches, not individual elements. This reduces park/unpark overhead.
		BlockingDeque<List<Object>>[] queues = new LinkedBlockingDeque[nbPartitions];
		{
			int queueCapacity = parameters.getQueueCapacity();
			for (int i = 0; i < nbPartitions; i++) {
				queues[i] = new LinkedBlockingDeque<>(queueCapacity);
			}
		}

		// Empty list sentinel signals end-of-stream to consumer threads
		List<Object> poison = List.of();

		// One consumer thread per partition — each drains batches sequentially
		var executor = parameters.getExecutor();
		var consumer = parameters.getConsumer();
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

		// Per-partition accumulation buffers on the producer thread (no synchronization needed)
		List<Object>[] buffers = new List[nbPartitions];
		for (int i = 0; i < nbPartitions; i++) {
			buffers[i] = new ArrayList<>(batchSize);
		}

		// Drive iteration on the current thread; batch elements by partition before dispatching
		try {
			var partitioner = parameters.getPartitioner();
			parameters.getStream().forEach(element -> {
				if (firstError.get() != null) {
					return;
				}
				int idx = partitioner.applyAsInt(element);
				buffers[idx].add(element);
				if (buffers[idx].size() == batchSize) {
					flushBuffer(queues[idx], buffers[idx], firstError);
					buffers[idx] = new ArrayList<>(batchSize);
				}
			});
			// Flush remaining partial batches
			for (int i = 0; i < nbPartitions; i++) {
				if (!buffers[i].isEmpty()) {
					flushBuffer(queues[i], buffers[i], firstError);
				}
			}
		} catch (RuntimeException e) {
			firstError.compareAndSet(null, e);
		} finally {
			// Signal completion to all consumer threads
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
						queue.putFirst(poison);
					} else {
						// Normal completion: append poison after all data batches
						queue.put(poison);
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					firstError.compareAndSet(null, e);
				}
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				firstError.compareAndSet(null, e);
			}
		}

		Throwable error = firstError.get();
		if (error instanceof RuntimeException re) {
			throw re;
		} else if (error != null) {
			throw new IllegalStateException("Partition consumer failed", error);
		}
	}

	/**
	 * Puts the batch into the queue, applying backpressure if the queue is full.
	 */
	protected static void flushBuffer(BlockingDeque<List<Object>> queue,
			List<Object> batch,
			AtomicReference<Throwable> firstError) {
		try {
			queue.put(batch);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			firstError.compareAndSet(null, e);
		}
	}
}
