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
package eu.solven.adhoc.dataframe.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.dataframe.column.partitioned.PartitioningHelpers;
import eu.solven.adhoc.dataframe.column.partitioned.ShardingForEachParameters;
import eu.solven.adhoc.stream.IConsumingStream;

public class TestPartitioningHelpers {

	private IConsumingStream<String> streamOf(String... items) {
		return IConsumingStream.fromStream(Stream.of(items));
	}

	private IConsumingStream<Integer> streamOfInts(int... items) {
		return IConsumingStream.fromStream(IntStream.of(items).boxed());
	}

	@Test
	public void forEachPartitioned_routesByPartitionIndex() {
		ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
		List<List<Integer>> partitionResults = List.of(Collections.synchronizedList(new ArrayList<>()),
				Collections.synchronizedList(new ArrayList<>()),
				Collections.synchronizedList(new ArrayList<>()));

		// Elements 0..8, partitioned into 3 buckets by modulo
		IConsumingStream<Integer> stream = streamOfInts(0, 1, 2, 3, 4, 5, 6, 7, 8);
		PartitioningHelpers.shardingForEach(ShardingForEachParameters.<Integer>builder()
				.stream(stream)
				.nbPartitions(3)
				.partitioner(i -> i % 3)
				.consumer(i -> partitionResults.get(i % 3).add(i))
				.executor(executor)
				.build());

		Assertions.assertThat(partitionResults.get(0)).containsExactly(0, 3, 6);
		Assertions.assertThat(partitionResults.get(1)).containsExactly(1, 4, 7);
		Assertions.assertThat(partitionResults.get(2)).containsExactly(2, 5, 8);
	}

	@Test
	public void forEachPartitioned_closesStream() {
		ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
		AtomicInteger closed = new AtomicInteger();

		IConsumingStream<String> stream = streamOf("a", "b").onClose(closed::incrementAndGet);
		PartitioningHelpers.shardingForEach(ShardingForEachParameters.<String>builder()
				.stream(stream)
				.nbPartitions(2)
				.partitioner(s -> s.charAt(0) % 2)
				.consumer(s -> {
				})
				.executor(executor)
				.build());

		Assertions.assertThat(closed.get()).isEqualTo(1);
	}

	@Test
	public void forEachPartitioned_propagatesConsumerException() {
		ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

		IConsumingStream<Integer> stream = streamOfInts(0, 1, 2);
		Assertions.assertThatThrownBy(
				() -> PartitioningHelpers.shardingForEach(ShardingForEachParameters.<Integer>builder()
						.stream(stream)
						.nbPartitions(2)
						.partitioner(i -> i % 2)
						.consumer(i -> {
							if (i == 1) {
								throw new RuntimeException("boom");
							}
						})
						.executor(executor)
						.build()))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("boom");
	}

	@Test
	public void forEachPartitioned_emptyStream() {
		ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
		AtomicInteger count = new AtomicInteger();

		IConsumingStream<Integer> stream = streamOfInts();
		PartitioningHelpers.shardingForEach(ShardingForEachParameters.<Integer>builder()
				.stream(stream)
				.nbPartitions(4)
				.partitioner(i -> i % 4)
				.consumer(i -> count.incrementAndGet())
				.executor(executor)
				.build());

		Assertions.assertThat(count.get()).isZero();
	}
}
