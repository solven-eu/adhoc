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
package eu.solven.adhoc.dataframe.row;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.dataframe.stream.SuppliedTabularRecordConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;

/**
 * Tests for {@link CompositeTabularRecordStream}.
 *
 * @author Benoit Lacelle
 */
public class TestCompositeTabularRecordStream {

	@Test
	public void testIsDistinctSlices_empty() {
		CompositeTabularRecordStream composite = CompositeTabularRecordStream.builder().build();

		Assertions.assertThat(composite.isDistinctSlices()).isTrue();
	}

	@Test
	public void testIsDistinctSlices_singleDistinct() {
		ITabularRecordStream distinct = new SuppliedTabularRecordConsumingStream("s", true, IConsumingStream::empty);

		CompositeTabularRecordStream composite = CompositeTabularRecordStream.builder().underlying(distinct).build();

		Assertions.assertThat(composite.isDistinctSlices()).isTrue();
	}

	@Test
	public void testIsDistinctSlices_singleNonDistinct() {
		ITabularRecordStream nonDistinct =
				new SuppliedTabularRecordConsumingStream("s", false, IConsumingStream::empty);

		CompositeTabularRecordStream composite = CompositeTabularRecordStream.builder().underlying(nonDistinct).build();

		Assertions.assertThat(composite.isDistinctSlices()).isFalse();
	}

	@Test
	public void testIsDistinctSlices_twoDistinct() {
		ITabularRecordStream distinct1 = new SuppliedTabularRecordConsumingStream("s1", true, IConsumingStream::empty);
		ITabularRecordStream distinct2 = new SuppliedTabularRecordConsumingStream("s2", true, IConsumingStream::empty);

		CompositeTabularRecordStream composite =
				CompositeTabularRecordStream.builder().underlying(distinct1).underlying(distinct2).build();

		// Two distinct streams may have overlapping slices, so the composite is not guaranteed distinct
		Assertions.assertThat(composite.isDistinctSlices()).isFalse();
	}

	static ITabularRecordStream streamThatClosesNormally(AtomicInteger closeCount) {
		return new ITabularRecordStream() {

			@Override
			public IConsumingStream<ITabularRecord> records() {
				return IConsumingStream.empty();
			}

			@Override
			public boolean isDistinctSlices() {
				return false;
			}

			@Override
			public void close() {
				closeCount.incrementAndGet();
			}
		};
	}

	static ITabularRecordStream streamThatThrowsOnClose(AtomicInteger closeCount, String message) {
		return new ITabularRecordStream() {
			@Override
			public IConsumingStream<ITabularRecord> records() {
				return IConsumingStream.empty();
			}

			@Override
			public boolean isDistinctSlices() {
				return false;
			}

			@Override
			public void close() {
				closeCount.incrementAndGet();
				throw new RuntimeException(message);
			}
		};
	}

	@Test
	public void testCloseAll_noStreams() {
		// Should not throw
		CompositeTabularRecordStream.closeAll(List.of());
	}

	@Test
	public void testCloseAll_allSucceed() {
		AtomicInteger closed = new AtomicInteger();
		CompositeTabularRecordStream
				.closeAll(List.of(streamThatClosesNormally(closed), streamThatClosesNormally(closed)));

		Assertions.assertThat(closed).hasValue(2);
	}

	@Test
	public void testCloseAll_firstThrows_secondStillClosed() {
		AtomicInteger closed = new AtomicInteger();

		Assertions
				.assertThatThrownBy(() -> CompositeTabularRecordStream
						.closeAll(List.of(streamThatThrowsOnClose(closed, "first"), streamThatClosesNormally(closed))))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("first");

		Assertions.assertThat(closed).hasValue(2);
	}

	@Test
	public void testCloseAll_bothThrow_secondIsSuppressed() {
		AtomicInteger closed = new AtomicInteger();

		Assertions
				.assertThatThrownBy(() -> CompositeTabularRecordStream.closeAll(
						List.of(streamThatThrowsOnClose(closed, "first"), streamThatThrowsOnClose(closed, "second"))))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("first")
				.satisfies(e -> Assertions.assertThat(e.getSuppressed())
						.hasSize(1)
						.anySatisfy(s -> Assertions.assertThat(s).hasMessage("second")));

		Assertions.assertThat(closed).hasValue(2);
	}
}
