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
package eu.solven.adhoc.stream.partitioned;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.stream.IConsumingStream;

public class TestPartitionedConsumingStream {

	@Test
	public void testGetNbPartitions_matchesBuilderInputs() {
		PartitionedConsumingStream<Integer> stream = PartitionedConsumingStream.<Integer>builder()
				.partition(IConsumingStream.fromStream(Stream.of(1, 2)))
				.partition(IConsumingStream.fromStream(Stream.of(3)))
				.build();

		Assertions.assertThat(stream.getNbPartitions()).isEqualTo(2);
	}

	@Test
	public void testGetPartition_returnsTheRequestedPartition() {
		IConsumingStream<Integer> p0 = IConsumingStream.fromStream(Stream.of(10));
		IConsumingStream<Integer> p1 = IConsumingStream.fromStream(Stream.of(20));

		PartitionedConsumingStream<Integer> stream =
				PartitionedConsumingStream.<Integer>builder().partition(p0).partition(p1).build();

		Assertions.assertThat(stream.getPartition(0)).isSameAs(p0);
		Assertions.assertThat(stream.getPartition(1)).isSameAs(p1);
	}

	@Test
	public void testForEach_visitsEveryPartitionInOrder() {
		PartitionedConsumingStream<Integer> stream = PartitionedConsumingStream.<Integer>builder()
				.partition(IConsumingStream.fromStream(Stream.of(1, 2)))
				.partition(IConsumingStream.fromStream(Stream.of(3, 4)))
				.partition(IConsumingStream.fromStream(Stream.of(5)))
				.build();

		List<Integer> seen = new ArrayList<>();
		stream.forEach(seen::add);

		Assertions.assertThat(seen).containsExactly(1, 2, 3, 4, 5);
	}

	@Test
	public void testForEach_zeroPartitions_emitsNothing() {
		PartitionedConsumingStream<Integer> stream = PartitionedConsumingStream.<Integer>builder().build();

		List<Integer> seen = new ArrayList<>();
		stream.forEach(seen::add);

		Assertions.assertThat(seen).isEmpty();
		Assertions.assertThat(stream.getNbPartitions()).isZero();
	}

	@Test
	public void testClose_propagatesToEveryPartition() {
		AtomicInteger closed = new AtomicInteger();

		IConsumingStream<Integer> p0 = countingClose(closed);
		IConsumingStream<Integer> p1 = countingClose(closed);

		PartitionedConsumingStream<Integer> stream =
				PartitionedConsumingStream.<Integer>builder().partition(p0).partition(p1).build();

		stream.close();

		Assertions.assertThat(closed).hasValue(2);
	}

	private static IConsumingStream<Integer> countingClose(AtomicInteger counter) {
		return new IConsumingStream<Integer>() {
			@Override
			public void forEach(java.util.function.Consumer<Integer> consumer) {
				// no elements
			}

			@Override
			public void close() {
				counter.incrementAndGet();
			}
		};
	}
}
