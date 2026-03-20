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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestSuppliedTabularRecordStream {

	@Test
	public void testGetTableQuery() {
		Object source = "mySource";
		SuppliedTabularRecordStream stream = new SuppliedTabularRecordStream(source, false, Stream::empty);

		Assertions.assertThat(stream.getTableQuery()).isSameAs(source);
	}

	@Test
	public void testIsDistinctSlices() {
		Assertions.assertThat(new SuppliedTabularRecordStream("s", true, Stream::empty).isDistinctSlices()).isTrue();
		Assertions.assertThat(new SuppliedTabularRecordStream("s", false, Stream::empty).isDistinctSlices()).isFalse();
	}

	@Test
	public void testRecords_delegatesToSupplier() {
		ITabularRecord record = Mockito.mock(ITabularRecord.class);
		SuppliedTabularRecordStream stream = new SuppliedTabularRecordStream("s", false, () -> Stream.of(record));

		Assertions.assertThat(stream.records()).containsExactly(record);
	}

	@Test
	public void testRecords_memoized() {
		// The supplier must be called at most once; subsequent calls return the same stream instance.
		AtomicInteger callCount = new AtomicInteger();
		SuppliedTabularRecordStream stream = new SuppliedTabularRecordStream("s", false, () -> {
			callCount.incrementAndGet();
			return Stream.empty();
		});

		stream.records();
		stream.records();

		Assertions.assertThat(callCount.get()).isEqualTo(1);
	}

	@Test
	public void testToString_containsSource() {
		SuppliedTabularRecordStream stream = new SuppliedTabularRecordStream("myQuery", false, Stream::empty);

		Assertions.assertThat(stream.toString()).contains("myQuery");
	}
}
