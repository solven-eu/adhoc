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

import eu.solven.adhoc.dataframe.column.partitioned.PartitionedForEachParameters;
import eu.solven.adhoc.dataframe.column.partitioned.PartitioningHelpers;
import eu.solven.adhoc.stream.IConsumingStream;

public class TestConsumingStream {

	private IConsumingStream<String> streamOf(String... items) {
		return IConsumingStream.fromStream(Stream.of(items));
	}

	private IConsumingStream<Integer> streamOfInts(int... items) {
		return IConsumingStream.fromStream(IntStream.of(items).boxed());
	}

	// forEach always closes the stream on exit (finally), so onClose hooks fire even on empty streams.
	@Test
	public void onClose_firedAutomaticallyOnEmptyStream() {
		AtomicInteger closed = new AtomicInteger();

		streamOf().onClose(closed::incrementAndGet).forEach(s -> {
		});

		Assertions.assertThat(closed.get()).isEqualTo(1);
	}

	@Test
	public void peek_calledBeforeMainConsumer() {
		List<String> order = new ArrayList<>();

		streamOf("x").peek(s -> order.add("peek:" + s)).forEach(s -> order.add("consume:" + s));

		Assertions.assertThat(order).containsExactly("peek:x", "consume:x");
	}

	@Test
	public void count_returnsNumberOfElements() {
		Assertions.assertThat(streamOf("a", "b", "c").count()).isEqualTo(3);
	}

	@Test
	public void filter_keepsMatchingElements() {
		List<String> result = new ArrayList<>();

		streamOf("apple", "banana", "avocado", "cherry").filter(s -> s.startsWith("a")).forEach(result::add);

		Assertions.assertThat(result).containsExactly("apple", "avocado");
	}

	@Test
	public void map_transformsElements() {
		List<String> result = new ArrayList<>();

		streamOf("hello", "world").map(String::toUpperCase).forEach(result::add);

		Assertions.assertThat(result).containsExactly("HELLO", "WORLD");
	}

	@Test
	public void map_intToString() {
		List<String> result = new ArrayList<>();

		streamOfInts(1, 2, 3).map(i -> "item-" + i).forEach(result::add);

		Assertions.assertThat(result).containsExactly("item-1", "item-2", "item-3");
	}

	@Test
	public void filterThenMap_mapNotCalledOnFilteredElements() {
		List<String> result = new ArrayList<>();
		AtomicInteger mapCallCount = new AtomicInteger();

		// Only even numbers pass the filter; the map must not be invoked for odd numbers
		streamOfInts(1, 2, 3, 4).filter(i -> i % 2 == 0).map(i -> {
			mapCallCount.incrementAndGet();
			return "item-" + i;
		}).forEach(result::add);

		Assertions.assertThat(result).containsExactly("item-2", "item-4");
		Assertions.assertThat(mapCallCount.get()).isEqualTo(2);
	}

	@Test
	public void onClose_firedByForEach() {
		AtomicInteger closed = new AtomicInteger();

		IConsumingStream<String> stream = streamOf("a").onClose(closed::incrementAndGet);
		stream.forEach(s -> {
		});

		// forEach always closes in its finally block
		Assertions.assertThat(closed.get()).isEqualTo(1);

		// subsequent close() is a no-op (idempotent)
		stream.close();
		Assertions.assertThat(closed.get()).isEqualTo(1);
	}

	@Test
	public void onClose_multipleHooksFiredInReverseOrder() {
		List<Integer> order = new ArrayList<>();

		IConsumingStream<String> stream =
				streamOf("a").onClose(() -> order.add(1)).onClose(() -> order.add(2)).onClose(() -> order.add(3));
		stream.close();

		Assertions.assertThat(order).containsExactly(3, 2, 1);
	}

	@Test
	public void onClose_afterFilter_firedByForEach() {
		AtomicInteger closed = new AtomicInteger();

		IConsumingStream<String> stream =
				streamOf("apple", "banana").filter(s -> s.startsWith("a")).onClose(closed::incrementAndGet);
		stream.forEach(s -> {
		});

		Assertions.assertThat(closed.get()).isEqualTo(1);
	}

	@Test
	public void onClose_afterMap_firedByForEach() {
		AtomicInteger closed = new AtomicInteger();

		IConsumingStream<String> stream = streamOfInts(1, 2).map(i -> "item-" + i).onClose(closed::incrementAndGet);
		stream.forEach(s -> {
		});

		Assertions.assertThat(closed.get()).isEqualTo(1);
	}

	// forEach auto-closes the stream on exception, so onClose hooks fire without try-with-resources.
	@Test
	public void onClose_firedAutomaticallyOnForEachException() {
		AtomicInteger closed = new AtomicInteger();

		IConsumingStream<String> stream = streamOf("a").onClose(closed::incrementAndGet);

		Assertions.assertThatThrownBy(() -> stream.forEach(s -> {
			throw new RuntimeException("boom");
		})).isInstanceOf(RuntimeException.class);

		Assertions.assertThat(closed.get()).isEqualTo(1);
	}

	// try-with-resources is the recommended pattern: close is guaranteed even when forEach throws.
	@Test
	public void onClose_calledViaTryWithResourcesOnForEachException() {
		AtomicInteger closed = new AtomicInteger();

		Assertions.assertThatThrownBy(() -> {
			try (IConsumingStream<String> stream = streamOf("a", "b").onClose(closed::incrementAndGet)) {
				stream.forEach(s -> {
					throw new RuntimeException("boom");
				});
			}
		}).isInstanceOf(RuntimeException.class);

		Assertions.assertThat(closed.get()).isEqualTo(1);
	}

	@Test
	public void forEachPartitioned_routesByPartitionIndex() {
		ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
		List<List<Integer>> partitionResults = List.of(Collections.synchronizedList(new ArrayList<>()),
				Collections.synchronizedList(new ArrayList<>()),
				Collections.synchronizedList(new ArrayList<>()));

		// Elements 0..8, partitioned into 3 buckets by modulo
		IConsumingStream<Integer> stream = streamOfInts(0, 1, 2, 3, 4, 5, 6, 7, 8);
		PartitioningHelpers.forEachPartitioned(PartitionedForEachParameters.<Integer>builder()
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
		PartitioningHelpers.forEachPartitioned(PartitionedForEachParameters.<String>builder()
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
				() -> PartitioningHelpers.forEachPartitioned(PartitionedForEachParameters.<Integer>builder()
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
		PartitioningHelpers.forEachPartitioned(PartitionedForEachParameters.<Integer>builder()
				.stream(stream)
				.nbPartitions(4)
				.partitioner(i -> i % 4)
				.consumer(i -> count.incrementAndGet())
				.executor(executor)
				.build());

		Assertions.assertThat(count.get()).isZero();
	}

	@Test
	public void mapThenFilter_filtersOverMappedProperty() {
		List<String> result = new ArrayList<>();

		// Map to uppercase first, then filter on the mapped value (length > 3)
		streamOf("hi", "hello", "hey", "world").map(String::toUpperCase)
				.filter(s -> s.length() > 3)
				.forEach(result::add);

		Assertions.assertThat(result).containsExactly("HELLO", "WORLD");
	}

}
