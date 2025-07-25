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
package eu.solven.adhoc.filter;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;

public class TestNotFilter {
	@Test
	public void testNot() {
		Assertions.assertThat(NotFilter.not(IAdhocFilter.MATCH_ALL)).isEqualTo(IAdhocFilter.MATCH_NONE);
		Assertions.assertThat(NotFilter.not(IAdhocFilter.MATCH_NONE)).isEqualTo(IAdhocFilter.MATCH_ALL);
	}

	@Test
	public void testNotOr() {
		IAdhocFilter orFilter =
				NotFilter.not(OrFilter.or(ColumnFilter.isEqualTo("c", "c1"), ColumnFilter.isEqualTo("d", "d1")));

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
		IAdhocFilter notFilter = NotFilter.not(ColumnFilter.isEqualTo("c", "c1"));
		IAdhocFilter notNotFilter = NotFilter.not(notFilter);

		Assertions.assertThat(notNotFilter).isEqualTo(ColumnFilter.isEqualTo("c", "c1"));
	}

	@Test
	public void testNotNot_like() {
		IAdhocFilter notFilter = NotFilter.not(ColumnFilter.isLike("c", "a%"));
		IAdhocFilter notNotFilter = NotFilter.not(notFilter);

		Assertions.assertThat(notNotFilter).isEqualTo(ColumnFilter.isLike("c", "a%"));
	}

	@Test
	public void testNotNot_and() {
		IAdhocFilter and = AndFilter.builder()
				.filter(ColumnFilter.isLike("a", "a%"))
				.filter(ColumnFilter.isLike("b", "b%"))
				.build();
		Assertions.assertThat(and).isInstanceOf(AndFilter.class);

		IAdhocFilter notAnd = NotFilter.not(and);
		Assertions.assertThat(notAnd).isInstanceOf(NotFilter.class);

		IAdhocFilter notNotAnd = NotFilter.not(notAnd);
		Assertions.assertThat(notNotAnd).isEqualTo(and);
	}

}
