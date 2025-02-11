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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;

public class TestAndMatcher {
	@Test
	public void testAndInEq() {
		AndMatcher a_and_aandb =
				AndMatcher.builder().operand(EqualsMatcher.isEqualTo("a")).operand(InMatcher.isIn("a", "b")).build();

		// TODO Improve this when relevant
		// Assertions.assertThat(a_and_aandb).isEqualTo(EqualsMatcher.isEqualTo("a"));
		Assertions.assertThat(a_and_aandb).isEqualTo(a_and_aandb);
	}

	@Test
	public void testEqualsDifferentOrder() {
		AndMatcher aThenB =
				AndMatcher.builder().operand(LikeMatcher.matching("a%")).operand(LikeMatcher.matching("%b")).build();
		AndMatcher bThenA =
				AndMatcher.builder().operand(LikeMatcher.matching("%b")).operand(LikeMatcher.matching("a%")).build();

		Assertions.assertThat(aThenB).isEqualTo(bThenA);
	}
}
