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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;

public class TestFilterHelpers {

	@Test
	public void testGetValueMatcher_in() {
		ColumnFilter inV1OrV2 = ColumnFilter.builder().column("c").matching(Set.of("v1", "v2")).build();
		Assertions.assertThat(FilterHelpers.getValueMatcher(inV1OrV2, "c")).isEqualTo(InMatcher.isIn("v1", "v2"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(inV1OrV2, "unknownColumn"))
				.isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_and() {
		ISliceFilter like1OrLike2 = AndFilter.and(ColumnFilter.isLike("c1", "1%"), ColumnFilter.isLike("c2", "2%"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(like1OrLike2, "c1")).isEqualTo(LikeMatcher.matching("1%"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(like1OrLike2, "c2")).isEqualTo(LikeMatcher.matching("2%"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(like1OrLike2, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_ALL_NONE() {
		Assertions.assertThat(FilterHelpers.getValueMatcher(ISliceFilter.MATCH_ALL, "c"))
				.isEqualTo(IValueMatcher.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.getValueMatcher(ISliceFilter.MATCH_NONE, "c"))
				.isEqualTo(IValueMatcher.MATCH_NONE);
	}

	@Test
	public void testGetValueMatcher_AND() {
		ISliceFilter filter = AndFilter.and(Map.of("c1", "v1", "c2", "v2"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1")).isEqualTo(EqualsMatcher.isEqualTo("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2")).isEqualTo(EqualsMatcher.isEqualTo("v2"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not() {
		ISliceFilter filter = NotFilter.not(AndFilter.and(Map.of("c1", "v1", "c2", "v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.isEqualTo("v1")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.isEqualTo("v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not_notOptimized() {
		ISliceFilter filterNotAll = NotFilter.builder().negated(ISliceFilter.MATCH_ALL).build();
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotAll, "c1")).isEqualTo(IValueMatcher.MATCH_NONE);
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotAll, "c2")).isEqualTo(IValueMatcher.MATCH_NONE);
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotAll, "c3")).isEqualTo(IValueMatcher.MATCH_NONE);

		ISliceFilter filterNotNone = NotFilter.builder().negated(ISliceFilter.MATCH_NONE).build();
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotNone, "c1")).isEqualTo(IValueMatcher.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotNone, "c2")).isEqualTo(IValueMatcher.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotNone, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not_inAnd() {
		ISliceFilter filter =
				AndFilter.and(ColumnFilter.isEqualTo("c1", "v1"), NotFilter.not(ColumnFilter.isEqualTo("c2", "v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1")).isEqualTo(EqualsMatcher.isEqualTo("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.isEqualTo("v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not_matcher() {
		ISliceFilter filter = AndFilter.and(Map.of("c1", "v1", "c2", NotMatcher.not(EqualsMatcher.isEqualTo("v2"))));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1")).isEqualTo(EqualsMatcher.isEqualTo("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.isEqualTo("v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetFilteredColumns_and() {
		ISliceFilter filter = AndFilter.and(Map.of("c1", "v1", "c2", "v2"));
		Assertions.assertThat(FilterHelpers.getFilteredColumns(filter)).isEqualTo(Set.of("c1", "c2"));
	}

	@Test
	public void testGetFilteredColumns_or() {
		ISliceFilter filter = OrFilter.or(Map.of("c1", "v1", "c2", "v2"));
		Assertions.assertThat(FilterHelpers.getFilteredColumns(filter)).isEqualTo(Set.of("c1", "c2"));
	}

	@Test
	public void testGetValueMatcher_or() {
		ISliceFilter filter = OrFilter.or(Map.of("c1", "v1", "c2", "v2"));
		Assertions.assertThatThrownBy(() -> FilterHelpers.getValueMatcher(filter, "c1"))
				.isInstanceOf(UnsupportedOperationException.class);

		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "unknown")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_or_lax() {
		ISliceFilter filter = OrFilter.or(AndFilter.and(Map.of("c1", "v1")),
				AndFilter.and(Map.of("c2", "v21")),
				AndFilter.and(Map.of("c2", "v22", "c3", "v3")));

		Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "c1")).isEqualTo(EqualsMatcher.isEqualTo("v1"));
		// Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "c2")).isEqualTo(InMatcher.isIn("v21",
		// "v22"));
		Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "c2"))
				.isEqualTo(OrMatcher.or(EqualsMatcher.isEqualTo("v21"), EqualsMatcher.isEqualTo("v22")));
		Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "unknown")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetFilteredColumns_not() {
		ISliceFilter filter = NotFilter.not(OrFilter.or(Map.of("c1", "v1", "c2", "v2")));
		Assertions.assertThat(FilterHelpers.getFilteredColumns(filter)).isEqualTo(Set.of("c1", "c2"));
	}

	@Test
	public void testWrapToString() throws JsonProcessingException {
		IValueMatcher wrappedWithToString = FilterHelpers.wrapWithToString(v -> "foo".equals(v), () -> "someToString");

		Assertions.assertThat(wrappedWithToString.toString()).isEqualTo("someToString");
		Assertions.assertThat(new ObjectMapper().writeValueAsString(wrappedWithToString)).isEqualTo("""
				{"type":"FilterHelpers$1","wrapped":"someToString"}""");

		Assertions.assertThat(wrappedWithToString.match("foo")).isTrue();
		Assertions.assertThat(wrappedWithToString.match("bar")).isFalse();
	}

	@Test
	public void testAsMap_or() {
		Assertions.assertThat(FilterHelpers.asMap(ColumnFilter.isEqualTo("c", "v"))).isEqualTo(Map.of("c", "v"));

		// BEWARE We may introduce a `toList` to manage OR.
		Assertions
				.assertThatThrownBy(() -> FilterHelpers
						.asMap(OrFilter.or(ColumnFilter.isEqualTo("c1", "v1"), ColumnFilter.isEqualTo("c2", "v2"))))
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThatThrownBy(() -> FilterHelpers.asMap(ISliceFilter.MATCH_NONE))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testSplitAnd() {
		Assertions.assertThat(FilterHelpers.splitAnd(ISliceFilter.MATCH_ALL)).containsExactly(ISliceFilter.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.splitAnd(ISliceFilter.MATCH_NONE)).containsExactly(ISliceFilter.MATCH_NONE);
		Assertions.assertThat(FilterHelpers.splitAnd(AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1"))))
				.containsExactly(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b1"));

		// IN is not an AND but an OR
		Assertions.assertThat(FilterHelpers.splitAnd(ColumnFilter.isIn("a", "a1", "a2")))
				.containsExactly(ColumnFilter.isIn("a", "a1", "a2"));
	}

	@Test
	public void testStripFilterFromWhere() {
		// filter is hold in where
		Assertions
				.assertThat(FilterHelpers.stripWhereFromFilter(AndFilter.and(Map.of("a", "a1")),
						AndFilter.and(Map.of("a", "a1"))))
				.isEqualTo(ISliceFilter.MATCH_ALL);

		// filter is unrelated with where
		Assertions
				.assertThat(FilterHelpers.stripWhereFromFilter(AndFilter.and(Map.of("a", "a1")),
						AndFilter.and(Map.of("b", "b1"))))
				.isEqualTo(AndFilter.and(Map.of("b", "b1")));

		// filter is laxer than where
		Assertions.assertThat(FilterHelpers.stripWhereFromFilter(AndFilter.and(Map.of("a", "a1", "b", "b1")),
				AndFilter.and(Map.of("b", "b1")))).isEqualTo(ISliceFilter.MATCH_ALL);

		// filter is disjoint with non-empty-union than where
		Assertions.assertThat(FilterHelpers.stripWhereFromFilter(AndFilter.and(Map.of("a", "a1", "b", "b1")),
				AndFilter.and(Map.of("b", "b1", "c", "c1")))).isEqualTo(AndFilter.and(Map.of("c", "c1")));
	}

	@Test
	public void testStripFilterFromWhere_NotOr() {
		ISliceFilter notA_and_notB = AndFilter.and(NotFilter.not(ColumnFilter.isEqualTo("a", "a1")),
				NotFilter.not(ColumnFilter.isEqualTo("b", "b1")),
				NotFilter.not(ColumnFilter.isEqualTo("c", "c1")));

		// Ensure the given is optimized into a Not(Or(a=a1|b=b1))
		Assertions.assertThat(notA_and_notB).isInstanceOf(NotFilter.class);

		// filter is hold in where
		Assertions
				.assertThat(FilterHelpers.stripWhereFromFilter(NotFilter.not(ColumnFilter.isEqualTo("a", "a1")),
						notA_and_notB))
				.isEqualTo(AndFilter.and(NotFilter.not(ColumnFilter.isEqualTo("b", "b1")),
						NotFilter.not(ColumnFilter.isEqualTo("c", "c1"))));
	}

	// This case may underline a subtle edge-case:
	// In this process, we will try each FILTER operator with the WHERE, to see if they are part of the WHERE or not.
	// This process may fail due to optimizations, and and `AND(...)` may be turned into a `!OR(...)`, which are then
	// considered not equals
	@Test
	public void testStripFilterFromWhere_NotAnd() {
		List<ISliceFilter> baseConditions =
				List.of(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b1"));

		// Add more and more negated conditions to push `AND(...)` to be turned into a `NOT(OR(...))`
		for (int nbNegated = 1; nbNegated < 8; nbNegated++) {
			List<ISliceFilter> allConditions = new ArrayList<>();

			allConditions.addAll(baseConditions);

			for (int j = 0; j < nbNegated; j++) {
				allConditions.add(NotFilter.not(ColumnFilter.isEqualTo("k" + j, "v" + j)));
			}

			ISliceFilter notA_and_notB = AndFilter.and(allConditions);

			// Ensure the given filter is still an AND
			Assertions.assertThat(notA_and_notB).isInstanceOf(AndFilter.class);

			// Ensure that even with many negated filters, the AND may turn an NOT(OR), but this algorithm does not fail
			Assertions
					.assertThat(FilterHelpers.isStricterThan(notA_and_notB,
							NotFilter.not(ColumnFilter.isEqualTo("k" + 0, "v" + 0))))
					.describedAs("nbNegated == %s", nbNegated)
					.isTrue();
		}
	}

}
