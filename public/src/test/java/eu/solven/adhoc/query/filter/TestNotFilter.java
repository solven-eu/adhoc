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
package eu.solven.adhoc.query.filter;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;

public class TestNotFilter {
	@Test
	public void testNot() {
		Assertions.assertThat(NotFilter.not(ISliceFilter.MATCH_ALL)).isEqualTo(ISliceFilter.MATCH_NONE);
		Assertions.assertThat(NotFilter.not(ISliceFilter.MATCH_NONE)).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testNotOr() {
		ISliceFilter orFilter = NotFilter
				.not(FilterBuilder.or(ColumnFilter.isEqualTo("c", "c1"), ColumnFilter.isEqualTo("d", "d1")).combine());

		Assertions.assertThat(orFilter).isInstanceOfSatisfying(AndFilter.class, andMatcher -> {
			Assertions.assertThat(andMatcher.getOperands())
					.hasSize(2)
					.contains(ColumnFilter.builder()
							.column("c")
							.valueMatcher(NotMatcher.not(EqualsMatcher.isEqualTo("c1")))
							.build())
					.contains(ColumnFilter.builder()
							.column("d")
							.valueMatcher(NotMatcher.not(EqualsMatcher.isEqualTo("d1")))
							.build());
		});
	}

	@Test
	public void testNotNot_equals() {
		ISliceFilter notFilter = NotFilter.not(ColumnFilter.isEqualTo("c", "c1"));
		ISliceFilter notNotFilter = NotFilter.not(notFilter);

		Assertions.assertThat(notNotFilter).isEqualTo(ColumnFilter.isEqualTo("c", "c1"));
	}

	@Test
	public void testNotNot_like() {
		ISliceFilter notFilter = NotFilter.not(ColumnFilter.isLike("c", "a%"));
		ISliceFilter notNotFilter = NotFilter.not(notFilter);

		Assertions.assertThat(notNotFilter).isEqualTo(ColumnFilter.isLike("c", "a%"));
	}

	@Test
	public void testNotNot_and() {
		ISliceFilter and =
				AndFilter.builder().and(ColumnFilter.isLike("a", "a%")).and(ColumnFilter.isLike("b", "b%")).build();
		Assertions.assertThat(and).isInstanceOf(AndFilter.class);

		ISliceFilter notAnd = NotFilter.not(and);
		Assertions.assertThat(notAnd).isInstanceOf(NotFilter.class);

		ISliceFilter notNotAnd = NotFilter.not(notAnd);
		Assertions.assertThat(notNotAnd).isEqualTo(and);
	}

}
