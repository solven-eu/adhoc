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
	@SuppressWarnings({ "unchecked", "PMD.CompareObjectsWithEquals" })
	public static <T> void forEachPartitioned(PartitionedForEachParameters<T> parameters) {
		int nbPartitions = parameters.getNbPartitions();

		// Sentinel object: identity-compared, never equals a real element
		Object poison = new Object();
		AtomicReference<Throwable> firstError = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(nbPartitions);

		BlockingDeque<Object>[] queues = new BlockingDeque[nbPartitions];
		{
			int queueCapacity = parameters.getQueueCapacity();
			for (int i = 0; i < nbPartitions; i++) {
				queues[i] = new LinkedBlockingDeque<>(queueCapacity);
			}
		}

		// One consumer thread per partition — each drains its queue sequentially
		var executor = parameters.getExecutor();
		var consumer = parameters.getConsumer();
		for (int i = 0; i < nbPartitions; i++) {
			BlockingDeque<Object> queue = queues[i];
			executor.execute(() -> {
				try {
					while (true) {
						Object item = queue.take();
						if (item == poison) {
							return;
						}
						consumer.accept((T) item);
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

		// Drive iteration on the current thread; dispatch each element to its partition's queue
		try {
			var partitioner = parameters.getPartitioner();
			parameters.getStream().forEach(element -> {
				if (firstError.get() != null) {
					// A consumer thread failed — skip remaining elements
					return;
				}
				int idx = partitioner.applyAsInt(element);
				try {
					queues[idx].put(element);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					firstError.compareAndSet(null, e);
				}
			});
		} catch (RuntimeException e) {
			firstError.compareAndSet(null, e);
		} finally {
			// Signal completion to all consumer threads
			for (int i = 0; i < nbPartitions; i++) {
				BlockingDeque<Object> queue = queues[i];
				try {
					int discarded = queue.size();
					if (discarded > 0) {
						log.warn("Discarding {} pending elements for partitionIndex={}", discarded, i);
					}
					// Clear pending elements to free capacity and avoid processing stale data,
					// then putFirst so the poison is the next item the consumer sees
					queue.clear();
					queue.putFirst(poison);
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
}
