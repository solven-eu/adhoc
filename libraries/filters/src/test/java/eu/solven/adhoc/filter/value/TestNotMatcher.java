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

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;

public class TestNotMatcher {
	@Test
	public void testNotLike() {
		IValueMatcher likeMatcher = LikeMatcher.matching("likeThis");
		IValueMatcher not = NotMatcher.not(likeMatcher);
		Assertions.assertThat(not)
				.isInstanceOfSatisfying(NotMatcher.class,
						negated -> Assertions.assertThat(negated.getNegated()).isSameAs(likeMatcher));
	}

	@Test
	public void testNotNot() {
		IValueMatcher likeMatcher = LikeMatcher.matching("likeThis");
		IValueMatcher not = NotMatcher.not(likeMatcher);
		IValueMatcher notNot = NotMatcher.not(not);
		Assertions.assertThat(notNot).isSameAs(likeMatcher);
	}

	@Test
	public void testNotComparing() {
		for (boolean someBoolean : Arrays.asList(true, false)) {
			ComparingMatcher comparingMatcher = ComparingMatcher.builder()
					.greaterThan(someBoolean)
					.matchIfEqual(someBoolean)
					.matchIfNull(someBoolean)
					.operand("someOperand")
					.build();
			IValueMatcher not = NotMatcher.not(comparingMatcher);
			Assertions.assertThat(not).isInstanceOfSatisfying(ComparingMatcher.class, negated -> {
				Assertions.assertThat(negated.getOperand()).isSameAs(comparingMatcher.getOperand());

				Assertions.assertThat(negated.isGreaterThan()).isEqualTo(!someBoolean);
				Assertions.assertThat(negated.isMatchIfEqual()).isEqualTo(!someBoolean);
				Assertions.assertThat(negated.isMatchIfNull()).isEqualTo(!someBoolean);
			});
		}
	}
}
