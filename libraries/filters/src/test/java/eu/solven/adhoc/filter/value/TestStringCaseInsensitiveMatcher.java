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
package eu.solven.adhoc.filter.value;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStringCaseInsensitiveMatcher {

	@Test
	public void testMatch_exact() {
		IValueMatcher matcher = StringCaseInsensitiveMatcher.builder().string("Hello").build();

		Assertions.assertThat(matcher.match("Hello")).isTrue();
	}

	@Test
	public void testMatch_differentCase() {
		IValueMatcher matcher = StringCaseInsensitiveMatcher.builder().string("Hello").build();

		Assertions.assertThat(matcher.match("hello")).isTrue();
		Assertions.assertThat(matcher.match("HELLO")).isTrue();
		Assertions.assertThat(matcher.match("hElLo")).isTrue();
	}

	@Test
	public void testMatch_noMatch() {
		IValueMatcher matcher = StringCaseInsensitiveMatcher.builder().string("Hello").build();

		Assertions.assertThat(matcher.match("World")).isFalse();
	}

	@Test
	public void testMatch_null() {
		IValueMatcher matcher = StringCaseInsensitiveMatcher.builder().string("Hello").build();

		Assertions.assertThat(matcher.match(null)).isFalse();
	}

	@Test
	public void testMatch_nonStringValue() {
		IValueMatcher matcher = StringCaseInsensitiveMatcher.builder().string("42").build();

		// Non-string values are matched via toString()
		Assertions.assertThat(matcher.match(42)).isTrue();
		Assertions.assertThat(matcher.match(43)).isFalse();
	}

	@Test
	public void testHasToString() {
		IValueMatcher matcher = StringCaseInsensitiveMatcher.hasToString(42);

		Assertions.assertThat(matcher.match("42")).isTrue();
		Assertions.assertThat(matcher.match("42".toUpperCase())).isTrue();
		Assertions.assertThat(matcher.match(43)).isFalse();
	}
}
