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
package eu.solven.adhoc.filter.value;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;

public class TestEqualsMatcher {
	@Test
	public void testSimple() {
		IValueMatcher equalsMatcher = EqualsMatcher.isEqualTo("a");

		Assertions.assertThat(equalsMatcher.match("a")).isEqualTo(true);
		Assertions.assertThat(equalsMatcher.match(Set.of("a"))).isEqualTo(false);

		Assertions.assertThat(equalsMatcher.match(null)).isEqualTo(false);
	}

	@Test
	public void testNull() {
		Assertions.assertThatThrownBy(() -> EqualsMatcher.builder().operand(null).build())
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThat(EqualsMatcher.isEqualTo(null)).isInstanceOf(NullMatcher.class);
	}
}
