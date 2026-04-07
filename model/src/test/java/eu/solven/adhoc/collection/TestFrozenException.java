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
package eu.solven.adhoc.collection;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFrozenException {

	@Test
	public void testMessage() {
		FrozenException ex = new FrozenException("structure is frozen");

		Assertions.assertThat(ex.getMessage()).isEqualTo("structure is frozen");
	}

	@Test
	public void testIsUnsupportedOperationException() {
		FrozenException ex = new FrozenException("frozen");

		Assertions.assertThat(ex).isInstanceOf(UnsupportedOperationException.class);
	}

	/** Callers that catch {@link UnsupportedOperationException} must also catch {@link FrozenException}. */
	@Test
	public void testCaughtAsUnsupportedOperationException() {
		Assertions.assertThatThrownBy(() -> {
			throw new FrozenException("write rejected");
		}).isInstanceOf(UnsupportedOperationException.class).hasMessage("write rejected");
	}
}
