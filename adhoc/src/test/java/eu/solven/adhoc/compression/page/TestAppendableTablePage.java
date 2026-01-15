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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestAppendableTablePage {
	public static final int TRY_RARE_RACE_CONDITIONS = 1024;

	@Test
	public void testCapacity() {
		AppendableTablePage page = AppendableTablePage.builder().capacity(2).build();

		Assertions.assertThat(page.pollNextRowIndex()).isEqualTo(0);
		Assertions.assertThat(page.pollNextRowIndex()).isEqualTo(1);
		Assertions.assertThat(page.pollNextRowIndex()).isEqualTo(-1);
		Assertions.assertThat(page.pollNextRowIndex()).isEqualTo(-1);
	}

	@Test
	public void testFreeze() {
		AppendableTablePage page = AppendableTablePage.builder().capacity(2).build();

		{
			ITableRowWrite row1 = page.pollNextRow();

			row1.add("k1", "v11");
			row1.add("k2", "v12");

			ITableRowRead rowFreeze1 = row1.freeze();
			Assertions.assertThat(rowFreeze1).hasToString("k1=v11, k2=v12");

			Assertions.assertThat(rowFreeze1.readValue(0)).isEqualTo("v11");
			Assertions.assertThat(rowFreeze1.readValue(1)).isEqualTo("v12");

			Assertions.assertThat(page.columnsWrite.get()).hasSize(2);
			Assertions.assertThat(page.columnsRead.get()).isSameAs(page.columnsWrite.get());
		}

		{
			ITableRowWrite row2 = page.pollNextRow();

			row2.add("k1", "v21");
			row2.add("k2", "v22");

			ITableRowRead rowFreeze2 = row2.freeze();
			Assertions.assertThat(rowFreeze2).hasToString("k1=v21, k2=v22");

			Assertions.assertThat(rowFreeze2.readValue(0)).isEqualTo("v21");
			Assertions.assertThat(rowFreeze2.readValue(1)).isEqualTo("v22");

			// Remove reference to the unfrozen column, to enable GC
			Assertions.assertThat(page.columnsWrite.get()).isNull();
			Assertions.assertThat(page.columnsRead.get()).hasSize(2);
		}

		Assertions.assertThat(page.pollNextRow()).isNull();
	}

	/**
	 * Checks the behavior when freezing happens during a read operation, which has to chose between the frozen and not
	 * frozen columns
	 *
	 * @throws InterruptedException
	 */
	// This test is not relevant anymore since the implementation is fully thread-safe, by relying on AtomicReference
	@Test
	public void testFreeze_concurrency() throws InterruptedException {
		try (ExecutorService es = Executors.newFixedThreadPool(2)) {

			// BEWARE This race-condition may require a long time before being observed. 1024 try to prevent spending
			// too much time observing a rare event
			for (int tryIndex = 0; tryIndex < TRY_RARE_RACE_CONDITIONS; tryIndex++) {
				AppendableTablePage page = AppendableTablePage.builder().capacity(2).build();

				ITableRowWrite row1 = page.pollNextRow();
				row1.add("a", "a1");
				ITableRowRead row1Frozen = row1.freeze();

				ITableRowWrite row2 = page.pollNextRow();

				AtomicReference<Throwable> refT = new AtomicReference<>();
				CountDownLatch cdl = new CountDownLatch(1);

				es.execute(() -> {
					try {
						row2.add("a", "a2");
						row2.freeze();

						cdl.countDown();
					} catch (Throwable t) {
						refT.set(t);
					}
				});

				es.execute(() -> {
					try {
						Assertions.assertThat(row1Frozen.readValue(0)).isEqualTo("a1");

						cdl.countDown();
					} catch (Throwable t) {
						refT.set(t);
					}
				});

				cdl.await();

				if (refT.get() != null) {
					Assertions.fail(refT.get());
				}
			}
		}
	}
}
