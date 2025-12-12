/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.engine.cancel;

import java.time.OffsetDateTime;

import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.context.IIsCancellable;

public class TestCancellationHelpers {
	@Test
	public void testAfterCancelled() {
		IIsCancellable isCancellable = new IIsCancellable() {

			@Override
			public boolean isCancelled() {
				return true;
			}

			@Override
			public OffsetDateTime getCancellationDate() {
				return OffsetDateTime.now().minusSeconds(7);
			}

			@Override
			public void addCancellationListener(Runnable runnable) {

			}

			@Override
			public void removeCancellationListener(Runnable runnable) {

			}
		};
		CancellationHelpers.afterCancellable(isCancellable);
	}
}
