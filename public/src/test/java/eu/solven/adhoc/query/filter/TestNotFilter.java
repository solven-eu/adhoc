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
import eu.solven.adhoc.query.filter.value.NullMatcher;

public class TestNotFilter {
	@Test
	public void testNot() {
		Assertions.assertThat(ISliceFilter.MATCH_ALL.negate()).isEqualTo(ISliceFilter.MATCH_NONE);
		Assertions.assertThat(ISliceFilter.MATCH_NONE.negate()).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testNotOr() {
		ISliceFilter orFilter =
				FilterBuilder.or(ColumnFilter.matchEq("c", "c1"), ColumnFilter.matchEq("d", "d1")).combine().negate();

		Assertions.assertThat(orFilter).isInstanceOfSatisfying(AndFilter.class, andMatcher -> {
			Assertions.assertThat(andMatcher.getOperands())
					.hasSize(2)
					.contains(ColumnFilter.builder()
							.column("c")
							.valueMatcher(NotMatcher.not(EqualsMatcher.matchEq("c1")))
							.build())
					.contains(ColumnFilter.builder()
							.column("d")
							.valueMatcher(NotMatcher.not(EqualsMatcher.matchEq("d1")))
							.build());
		});
	}

	@Test
	public void testNotNot_equals() {
		ISliceFilter notFilter = ColumnFilter.matchEq("c", "c1").negate();
		ISliceFilter notNotFilter = notFilter.negate();

		Assertions.assertThat(notNotFilter).isEqualTo(ColumnFilter.matchEq("c", "c1"));
	}

	@Test
	public void testNotNot_like() {
		ISliceFilter notFilter = ColumnFilter.matchLike("c", "a%").negate();
		ISliceFilter notNotFilter = notFilter.negate();

		Assertions.assertThat(notNotFilter).isEqualTo(ColumnFilter.matchLike("c", "a%"));
	}

	@Test
	public void testNotNot_and() {
		ISliceFilter and = AndFilter.builder()
				.and(ColumnFilter.matchLike("a", "a%"))
				.and(ColumnFilter.matchLike("b", "b%"))
				.build();
		Assertions.assertThat(and).isInstanceOf(AndFilter.class);

		ISliceFilter notAnd = and.negate();
		Assertions.assertThat(notAnd).isInstanceOf(OrFilter.class);

		ISliceFilter notNotAnd = notAnd.negate();
		Assertions.assertThat(notNotAnd).isEqualTo(and);
	}

	@Test
	public void testNot_nullMatcher() {
		ISliceFilter matchNull = ColumnFilter.match("c", NullMatcher.matchNull());
		Assertions.assertThat(matchNull).isInstanceOfSatisfying(ColumnFilter.class, f -> {
			Assertions.assertThat(f.isNullIfAbsent()).isTrue();
		});

		ISliceFilter notNotFilter = matchNull.negate();
		Assertions.assertThat(notNotFilter).isInstanceOfSatisfying(ColumnFilter.class, f -> {
			Assertions.assertThat(f.isNullIfAbsent()).isTrue();
		});

		ISliceFilter negatedFilter = matchNull.negate();
		Assertions.assertThat(negatedFilter).isInstanceOfSatisfying(ColumnFilter.class, f -> {
			Assertions.assertThat(f.isNullIfAbsent()).isTrue();
		});
	}

}
