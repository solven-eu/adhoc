package eu.solven.adhoc.table.arrow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ArrowPojoStreamer<T> {

	public static <T> void forEach(ArrowReader reader, Consumer<VectorSchemaRoot> batchMapper) {
		try (ListeningExecutorService vthreads =
				MoreExecutors.listeningDecorator(Executors.newVirtualThreadPerTaskExecutor())) {

			// Producer: read Arrow batches in a mono-threaded way
			Future<List<ListenableFuture<?>>> allTasks = vthreads.submit(() -> {
				List<ListenableFuture<?>> tasks = new ArrayList<>();

				// Batch loading is mono-threaded
				while (reader.loadNextBatch()) {
					VectorSchemaRoot root = reader.getVectorSchemaRoot();

					int nbRows = root.getRowCount();

					int partitionSize = (16 * 1024);
					int nbPartitions = nbRows / partitionSize;

					IntStream.range(0, nbPartitions).forEach(partitionIndex -> {
						int startIndex = partitionIndex * partitionSize;

						int actualPartitionSize;
						if (partitionIndex == nbPartitions - 1) {
							actualPartitionSize = nbRows - startIndex;
						} else {
							actualPartitionSize = partitionSize;
						}
						VectorSchemaRoot slice = root.slice(startIndex, actualPartitionSize);

						tasks.add(vthreads.submit(() -> {
							VectorSchemaRoot oneSlice = slice;

							try {
								batchMapper.accept(oneSlice);
							} finally {
								oneSlice.close();
							}
						}));
					});
				}

				return tasks;
			});

			List<ListenableFuture<?>> asList = Futures.getUnchecked(allTasks);
			List<?> asList2 = Futures.getUnchecked(Futures.allAsList(asList));

			log.debug("Complete {} tasks", asList2.size());

		}
	}
}