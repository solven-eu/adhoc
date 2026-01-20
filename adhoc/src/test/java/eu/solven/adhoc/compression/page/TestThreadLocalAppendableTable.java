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
package eu.solven.adhoc.compression.page;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.ThreadLocalAppendableTable;
import eu.solven.adhoc.map.factory.SequencedSetLikeList;

public class TestThreadLocalAppendableTable {
	@Test
	public void testSequential() {
		IAppendableTable table = ThreadLocalAppendableTable.builder().capacity(1).build();

		SequencedSetLikeList columns = SequencedSetLikeList.fromSet(Set.of("a"));

		{
			ITableRowWrite row = table.nextRow(columns);
			row.add("a", "a1");
			Assertions.assertThat(row.freeze().readValue(0)).isEqualTo("a1");
		}
		{
			ITableRowWrite row = table.nextRow(columns);
			row.add("a", "a2");
			Assertions.assertThat(row.freeze().readValue(0)).isEqualTo("a2");
		}
		{
			ITableRowWrite row = table.nextRow(columns);
			row.add("a", "a3");
			Assertions.assertThat(row.freeze().readValue(0)).isEqualTo("a3");
		}
	}

	@Test
	public void testConcurrency_capacity1() throws InterruptedException {
		int capacity = 1;
		concurrentWrites(capacity);
	}

	// having capacity=2 leads to having different threads trying to write the second row at the same time
	@Test
	public void testConcurrency_capacity2() throws InterruptedException {
		int capacity = 2;
		concurrentWrites(capacity);
	}

	private void concurrentWrites(int capacity) throws InterruptedException {
		IAppendableTable table = ThreadLocalAppendableTable.builder().capacity(capacity).build();

		SequencedSetLikeList columns = SequencedSetLikeList.fromSet(Set.of("a"));

		int concurrency = 8;
		ExecutorService es = Executors.newFixedThreadPool(concurrency);

		AtomicReference<Throwable> refFailure = new AtomicReference<>();

		AtomicInteger rowIndex = new AtomicInteger();
		IntStream.range(0, concurrency).forEach(concurrencyIndex -> {
			IntStream.range(0, 1024).forEach(pageRowIndex -> {
				es.execute(() -> {
					try {
						ITableRowWrite row = table.nextRow(columns);
						String value = "a" + rowIndex.getAndIncrement();
						row.add("a", value);
						Assertions.assertThat(row.freeze().readValue(0)).isEqualTo(value);
					} catch (Throwable t) {
						// Assertions.fail("concurrencyIndex=%s pageRowIndex=%s".formatted(concurrencyIndex,
						// pageRowIndex),
						// t);
						refFailure.set(t);
					}
				});
			});
		});

		es.shutdown();
		es.awaitTermination(1, TimeUnit.MINUTES);

		if (refFailure.get() != null) {
			Assertions.fail(refFailure.get());
		}
	}

}
