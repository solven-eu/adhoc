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
package eu.solven.adhoc.compression.column.freezer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.column.IntegerArrayColumn;
import eu.solven.adhoc.compression.column.ObjectArrayColumn;
import eu.solven.adhoc.compression.page.IReadableColumn;

public class TestAsynchronousFreezingStrategy {
	@Test
	public void testFreeze_defaultIsDirect() {
		AsynchronousFreezingStrategy strategy = AsynchronousFreezingStrategy.builder().build();

		ObjectArrayColumn column = ObjectArrayColumn.builder().asArray(List.of(1, 2, 3)).build();

		IReadableColumn frozen = column.freeze(strategy);

		Assertions.assertThat(frozen).isInstanceOfSatisfying(DynamicReadableColumn.class, d -> {
			Assertions.assertThat(d.ref.get()).isInstanceOf(IntegerArrayColumn.class);
		});
	}

	@Test
	public void testFreeze_executor() {
		try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
			AsynchronousFreezingStrategy strategy = AsynchronousFreezingStrategy.builder().executor(executor).build();

			ObjectArrayColumn column = ObjectArrayColumn.builder().asArray(List.of(1, 2, 3)).build();

			IReadableColumn frozen = column.freeze(strategy);

			Assertions.assertThat(frozen).isInstanceOfSatisfying(DynamicReadableColumn.class, d -> {
				Awaitility.await().untilAsserted(() -> {
					Assertions.assertThat(d.ref.get()).isInstanceOf(IntegerArrayColumn.class);
				});
			});
		}
	}

	@Test
	public void testFreeze_inJFP() {
		AtomicBoolean isTestDone = new AtomicBoolean();
		AtomicReference<Throwable> refE = new AtomicReference<>();

		ForkJoinPool.commonPool().submit(() -> {
			try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
				AsynchronousFreezingStrategy strategy =
						AsynchronousFreezingStrategy.builder().executor(executor).build();

				ObjectArrayColumn column = ObjectArrayColumn.builder().asArray(List.of(1, 2, 3)).build();

				IReadableColumn frozen = column.freeze(strategy);

				Assertions.assertThat(frozen).isInstanceOfSatisfying(DynamicReadableColumn.class, d -> {
					Awaitility.await().untilAsserted(() -> {
						Assertions.assertThat(d.ref.get()).isInstanceOf(IntegerArrayColumn.class);
						isTestDone.set(true);
					});
				});
			} catch (Throwable t) {
				refE.set(t);
			}
		}).join();

		Assertions.assertThat(isTestDone).isTrue();
		Assertions.assertThat(refE.get()).isNull();

	}
}
