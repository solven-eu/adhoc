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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Processes Arrow batches concurrently without using {@link Stream} or {@link Spliterator}.
 *
 * <h2>Why not {@link Stream#parallel()}?</h2>
 *
 * {@link Stream#parallel()} routes work through a {@link ForkJoinPool}. FJP is designed for CPU-bound work-stealing
 * where tasks split quickly and run without blocking. Arrow batch-loading is the opposite: it is inherently
 * mono-threaded (an {@link ArrowReader} must load batches sequentially) and is IO-bound (reading from disk or network).
 * Running it under FJP would either starve the pool (blocking carrier threads) or produce no parallelism at all.
 *
 * <h2>Why not {@link Spliterator}?</h2>
 *
 * {@link Stream} implementations call {@link Spliterator#trySplit()} eagerly, before any data has been loaded, to
 * divide work across threads. Because Arrow batches are not available until the reader advances, {@code trySplit()}
 * cannot return meaningful sub-spliterators early. The stream would fall back to sequential processing, defeating the
 * purpose.
 *
 * <h2>Design</h2>
 *
 * Batch loading runs on a single virtual thread. Once a batch is loaded its row slices are submitted as independent
 * tasks to a virtual-thread executor (the mixed pool), which is blocking-friendly and has no work-stealing pressure.
 * This keeps the mono-threaded constraint of Arrow intact while still overlapping CPU work across slices.
 *
 * @param <T>
 * @author Benoit Lacelle
 */
@Slf4j
@UtilityClass
public class ArrowPojoStreamer<T> {

	public static <T> void forEach(ArrowReader reader, Consumer<VectorSchemaRoot> batchMapper, int minSplitRows) {
		try (ListeningExecutorService vthreads =
				MoreExecutors.listeningDecorator(Executors.newVirtualThreadPerTaskExecutor())) {
			forEach(reader, batchMapper, minSplitRows, vthreads);
		}
	}

	public static void forEach(ArrowReader reader,
			Consumer<VectorSchemaRoot> batchMapper,
			int minSplitRows,
			ListeningExecutorService vthreads) {
		// Producer: read Arrow batches in a mono-threaded way
		Future<List<ListenableFuture<?>>> allTasks = vthreads.submit(() -> {
			List<ListenableFuture<?>> tasks = new ArrayList<>();

			// Batch loading is mono-threaded
			while (reader.loadNextBatch()) {
				forEachBatch(reader, batchMapper, minSplitRows, vthreads, tasks);
			}

			return tasks;
		});

		List<ListenableFuture<?>> asList = Futures.getUnchecked(allTasks);
		List<?> asList2 = Futures.getUnchecked(Futures.allAsList(asList));

		log.debug("Complete {} tasks", asList2.size());
	}

	@SuppressWarnings("PMD.CloseResource")
	private static void forEachBatch(ArrowReader reader,
			Consumer<VectorSchemaRoot> batchMapper,
			int minSplitRows,
			ListeningExecutorService vthreads,
			List<ListenableFuture<?>> tasks) throws IOException {
		VectorSchemaRoot root = reader.getVectorSchemaRoot();

		int nbRows = root.getRowCount();

		int partitionSize = minSplitRows;
		int nbPartitions = nbRows / partitionSize;

		IntStream.rangeClosed(0, nbPartitions).forEach(partitionIndex -> {
			int startIndex = partitionIndex * partitionSize;

			int actualPartitionSize;
			if (partitionIndex == nbPartitions) {
				actualPartitionSize = nbRows - startIndex;
			} else {
				actualPartitionSize = partitionSize;
			}
			VectorSchemaRoot slice = root.slice(startIndex, actualPartitionSize);

			// TODO Need to bound this for backpressure
			tasks.add(vthreads.submit(() -> {
				try (VectorSchemaRoot oneSlice = slice) {
					batchMapper.accept(oneSlice);
				}
			}));
		});
	}
}