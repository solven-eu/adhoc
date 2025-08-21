/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.query.filter;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;

public class TestLikeFilter {
	@Test
	public void testJustText() {
		IValueMatcher matcher = LikeMatcher.builder().pattern("a").build();

		Assertions.assertThat(matcher.match("a")).isTrue();
		Assertions.assertThat(matcher.match("A")).isTrue();
		Assertions.assertThat(matcher.match("a1")).isFalse();
		Assertions.assertThat(matcher.match("1a")).isFalse();
		Assertions.assertThat(matcher.match("1a1a1")).isFalse();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}

	@Test
	public void testStartsWith() {
		IValueMatcher matcher = LikeMatcher.builder().pattern("a%").build();

		Assertions.assertThat(matcher.match("a")).isTrue();
		Assertions.assertThat(matcher.match("A")).isTrue();
		Assertions.assertThat(matcher.match("a1")).isTrue();
		Assertions.assertThat(matcher.match("1a")).isFalse();

		Assertions.assertThat(matcher.match("1a1a1")).isFalse();
		Assertions.assertThat(matcher.match("1a1b1")).isFalse();
		Assertions.assertThat(matcher.match("1a1a1b1")).isFalse();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}

	@Test
	public void testContains() {
		IValueMatcher matcher = LikeMatcher.builder().pattern("%a%").build();

		Assertions.assertThat(matcher.match("a")).isTrue();
		Assertions.assertThat(matcher.match("A")).isTrue();
		Assertions.assertThat(matcher.match("a1")).isTrue();
		Assertions.assertThat(matcher.match("1a")).isTrue();

		Assertions.assertThat(matcher.match("1a1a1")).isTrue();
		Assertions.assertThat(matcher.match("1a1b1")).isTrue();
		Assertions.assertThat(matcher.match("1a1a1b1")).isTrue();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}

	@Test
	public void testContainsTwo() {
		IValueMatcher matcher = LikeMatcher.builder().pattern("%a%b%").build();

		Assertions.assertThat(matcher.match("a")).isFalse();
		Assertions.assertThat(matcher.match("A")).isFalse();
		Assertions.assertThat(matcher.match("a1")).isFalse();
		Assertions.assertThat(matcher.match("1a")).isFalse();

		Assertions.assertThat(matcher.match("1a1a1")).isFalse();
		Assertions.assertThat(matcher.match("1a1b1")).isTrue();
		Assertions.assertThat(matcher.match("1a1a1b1")).isTrue();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}

	@Test
	public void testTwiceThenOther() {
		IValueMatcher matcher = LikeMatcher.builder().pattern("%a%a%b%").build();

		Assertions.assertThat(matcher.match("a")).isFalse();
		Assertions.assertThat(matcher.match("A")).isFalse();
		Assertions.assertThat(matcher.match("a1")).isFalse();
		Assertions.assertThat(matcher.match("1a")).isFalse();

		Assertions.assertThat(matcher.match("1a1a1")).isFalse();
		Assertions.assertThat(matcher.match("1a1b1")).isFalse();
		Assertions.assertThat(matcher.match("1a1a1b1")).isTrue();

		Assertions.assertThat(matcher.match("b")).isFalse();
	}

	@Disabled("TODO")
	@Test
	public void testEscapeRegexSpecialChars() {
		IValueMatcher matcher = LikeMatcher.builder().pattern("a[b").build();

		Assertions.assertThat(matcher.match("a[b")).isTrue();
		Assertions.assertThat(matcher.match("A")).isFalse();
	}

	// https://stackoverflow.com/questions/19749787/how-to-use-a-percent-in-a-like-without-it-being-treated-as-a-wildcard
	@Disabled("TODO")
	@Test
	public void testEscapePercent() {
		IValueMatcher matcher = LikeMatcher.builder().pattern("%\\%%").build();

		Assertions.assertThat(matcher.match("a%b")).isTrue();
		Assertions.assertThat(matcher.match("ab")).isFalse();
	}
}
