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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import tools.jackson.databind.ObjectMapper;

public class TestFilterHelpers {

	@Test
	public void testGetValueMatcher_in() {
		ColumnFilter inV1OrV2 = ColumnFilter.builder().column("c").matching(Set.of("v1", "v2")).build();
		Assertions.assertThat(FilterHelpers.getValueMatcher(inV1OrV2, "c")).isEqualTo(InMatcher.matchIn("v1", "v2"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(inV1OrV2, "unknownColumn"))
				.isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_and() {
		ISliceFilter like1OrLike2 =
				AndFilter.and(ColumnFilter.matchLike("c1", "1%"), ColumnFilter.matchLike("c2", "2%"));
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
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1")).isEqualTo(EqualsMatcher.matchEq("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2")).isEqualTo(EqualsMatcher.matchEq("v2"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not() {
		ISliceFilter filter = NotFilter.builder().negated(AndFilter.and(Map.of("c1", "v1", "c2", "v2"))).build();
		Assertions.assertThatThrownBy(() -> FilterHelpers.getValueMatcher(filter, "c1"))
				.isInstanceOf(UnsupportedOperationException.class);
		Assertions.assertThatThrownBy(() -> FilterHelpers.getValueMatcher(filter, "c2"))
				.isInstanceOf(UnsupportedOperationException.class);
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_OrNot() {
		ISliceFilter filter = AndFilter.and(Map.of("c1", "v1", "c2", "v2")).negate();
		Assertions.assertThatThrownBy(() -> FilterHelpers.getValueMatcher(filter, "c1"))
				.isInstanceOf(UnsupportedOperationException.class);
		Assertions.assertThatThrownBy(() -> FilterHelpers.getValueMatcher(filter, "c2"))
				.isInstanceOf(UnsupportedOperationException.class);
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
				AndFilter.and(ColumnFilter.matchEq("c1", "v1"), ColumnFilter.matchEq("c2", "v2").negate());
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1")).isEqualTo(EqualsMatcher.matchEq("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.matchEq("v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not_matcher() {
		ISliceFilter filter = AndFilter.and(Map.of("c1", "v1", "c2", NotMatcher.not(EqualsMatcher.matchEq("v2"))));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1")).isEqualTo(EqualsMatcher.matchEq("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.matchEq("v2")));
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
		ISliceFilter filter =
				FilterBuilder
						.or(AndFilter.and(Map.of("c1", "v1")),
								AndFilter.and(Map.of("c2", "v21")),
								AndFilter.and(Map.of("c2", "v22", "c3", "v3")))
						.optimize();

		// c1 is matching all as, given an OR, any c1 is acceptable with `c1=v21`
		Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "c1")).isEqualTo(IValueMatcher.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "c2")).isEqualTo(IValueMatcher.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "unknown")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetFilteredColumns_not() {
		ISliceFilter filter = OrFilter.or(Map.of("c1", "v1", "c2", "v2")).negate();
		Assertions.assertThat(FilterHelpers.getFilteredColumns(filter)).isEqualTo(Set.of("c1", "c2"));
	}

	@Test
	public void testWrapToString() {
		IValueMatcher wrappedWithToString = FilterHelpers.wrapWithToString(v -> "foo".equals(v), () -> "someToString");

		Assertions.assertThat(wrappedWithToString.toString()).isEqualTo("someToString");
		Assertions.assertThat(new ObjectMapper().writeValueAsString(wrappedWithToString)).isEqualTo("""
				{"type":"FilterHelpers$1","wrapped":"someToString"}""");

		Assertions.assertThat(wrappedWithToString.match("foo")).isTrue();
		Assertions.assertThat(wrappedWithToString.match("bar")).isFalse();
	}

	@Test
	public void testAsMap_column() {
		Assertions.assertThat(FilterHelpers.asMap(ColumnFilter.matchEq("c", "v"))).isEqualTo(Map.of("c", "v"));
	}

	@Test
	public void testAsMap_and() {
		Assertions.assertThat(FilterHelpers.asMap(AndFilter.and(Map.of("a", "a1")))).isEqualTo(Map.of("a", "a1"));

		Assertions.assertThat(FilterHelpers.asMap(ISliceFilter.MATCH_ALL)).isEmpty();
	}

	@Test
	public void testAsMap_or() {
		// BEWARE We may introduce a `toList` to manage OR.
		Assertions.assertThatThrownBy(() -> FilterHelpers
				.asMap(FilterBuilder.or(ColumnFilter.matchEq("c1", "v1"), ColumnFilter.matchEq("c2", "v2")).optimize()))
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThatThrownBy(() -> FilterHelpers.asMap(ISliceFilter.MATCH_NONE))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testSplitAnd() {
		Assertions.assertThat(FilterHelpers.splitAnd(ISliceFilter.MATCH_ALL)).isEmpty();
		Assertions.assertThat(FilterHelpers.splitAnd(ISliceFilter.MATCH_NONE)).containsExactly(ISliceFilter.MATCH_NONE);
		Assertions.assertThat(FilterHelpers.splitAnd(AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1"))))
				.containsExactly(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b1"));

		// IN is not an AND but an OR
		Assertions.assertThat(FilterHelpers.splitAnd(ColumnFilter.matchIn("a", "a1", "a2")))
				.containsExactly(ColumnFilter.matchIn("a", "a1", "a2"));
	}

	@Test
	public void testSplitOr() {
		Assertions.assertThat(FilterHelpers.splitOr(ISliceFilter.MATCH_ALL)).contains(ISliceFilter.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.splitOr(ISliceFilter.MATCH_NONE)).isEmpty();
		Assertions.assertThat(FilterHelpers.splitOr(OrFilter.or(ImmutableMap.of("a", "a1", "b", "b1"))))
				.containsExactly(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b1"));

		// IN is an OR
		Assertions.assertThat(FilterHelpers.splitOr(ColumnFilter.matchIn("a", "a1", "a2")))
				.containsExactly(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("a", "a2"));
	}

	@Test
	public void testStripFilterFromWhere_same() {
		// filter is hold in where
		Assertions
				.assertThat(FilterHelpers.stripWhereFromFilter(AndFilter.and(Map.of("a", "a1")),
						AndFilter.and(Map.of("a", "a1"))))
				.isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testStripFilterFromWhere_unrelated() {
		// filter is unrelated with where
		Assertions
				.assertThat(FilterHelpers.stripWhereFromFilter(AndFilter.and(Map.of("a", "a1")),
						AndFilter.and(Map.of("b", "b1"))))
				.isEqualTo(AndFilter.and(Map.of("b", "b1")));
	}

	@Test
	public void testStripFilterFromWhere_filterIsLaxer() {
		// filter is laxer than where
		Assertions.assertThat(FilterHelpers.stripWhereFromFilter(AndFilter.and(Map.of("a", "a1", "b", "b1")),
				AndFilter.and(Map.of("b", "b1")))).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testStripFilterFromWhere_nonEmptyIntersection() {
		// filter is disjoint with non-empty-union than where
		Assertions.assertThat(FilterHelpers.stripWhereFromFilter(AndFilter.and(Map.of("a", "a1", "b", "b1")),
				AndFilter.and(Map.of("b", "b1", "c", "c1")))).isEqualTo(AndFilter.and(Map.of("c", "c1")));
	}

	@Test
	public void testStripFilterFromWhere_emptyIntersection() {
		// filter is disjoint with empty intersection
		Assertions.assertThat(FilterHelpers.stripWhereFromFilter(ColumnFilter.matchIn("a", "a1", "a2"),
				ColumnFilter.matchIn("a", "a3", "a4"))).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testStripFilterFromWhere_NotOr() {
		ISliceFilter notA_and_notB = AndFilter
				.and(ColumnFilter.notEq("a", "a1"), ColumnFilter.notEq("b", "b1"), ColumnFilter.notEq("c", "c1"));

		// Ensure the given is optimized into a Not(Or(a=a1|b=b1))
		Assertions.assertThat(notA_and_notB).isInstanceOf(AndFilter.class);

		// filter is hold in where
		Assertions.assertThat(FilterHelpers.stripWhereFromFilter(ColumnFilter.notEq("a", "a1"), notA_and_notB))
				.isEqualTo(AndFilter.and(ColumnFilter.notEq("b", "b1"), ColumnFilter.notEq("c", "c1")));
	}

	@Test
	public void testStripFilterFromWhere_whereEquals_filterGreaterThan_orOther() {
		ISliceFilter equalsTo = ColumnFilter.matchEq("a", 123);
		ISliceFilter greaterThanOrOther = FilterBuilder
				.or(ColumnFilter.match("a", ComparingMatcher.strictlyGreaterThan(12)), ColumnFilter.matchEq("b", "b1"))
				.combine();

		Assertions.assertThat(FilterHelpers.stripWhereFromFilter(equalsTo, greaterThanOrOther))
				.isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testStripFilterFromWhere_whereEquals_filterGreaterThan_andOther() {
		ISliceFilter equalsTo = ColumnFilter.matchEq("a", 123);
		ISliceFilter greaterThanOrOther = FilterBuilder
				.and(ColumnFilter.match("a", ComparingMatcher.strictlyGreaterThan(12)), ColumnFilter.matchEq("b", "b1"))
				.combine();

		Assertions.assertThat(FilterHelpers.stripWhereFromFilter(equalsTo, greaterThanOrOther))
				.isEqualTo(ColumnFilter.matchEq("b", "b1"));
	}

	@Test
	public void testStripFilterFromWhere_matchNone_filterNatchNone() {
		Assertions.assertThat(FilterHelpers.stripWhereFromFilter(ISliceFilter.MATCH_NONE, ISliceFilter.MATCH_NONE))
				.isEqualTo(ISliceFilter.MATCH_ALL);
	}

	// This case may underline a subtle edge-case:
	// In this process, we will try each FILTER operator with the WHERE, to see if they are part of the WHERE or not.
	// This process may fail due to optimizations, and and `AND(...)` may be turned into a `!OR(...)`, which are then
	// considered not equals
	@Test
	public void testStripFilterFromWhere_NotAnd() {
		List<ISliceFilter> baseConditions = List.of(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b1"));

		// Add more and more negated conditions to push `AND(...)` to be turned into a `NOT(OR(...))`
		for (int nbNegated = 1; nbNegated < 8; nbNegated++) {
			List<ISliceFilter> allConditions = new ArrayList<>();

			allConditions.addAll(baseConditions);

			for (int j = 0; j < nbNegated; j++) {
				allConditions.add(ColumnFilter.matchEq("k" + j, "v" + j).negate());
			}

			ISliceFilter notA_and_notB = AndFilter.and(allConditions);

			// Ensure the given filter is still an AND
			// Assertions.assertThat(notA_and_notB)
			// .describedAs("nbNegated == %s", nbNegated)
			// .isInstanceOf(AndFilter.class);

			// Ensure that even with many negated filters, the AND may turn an NOT(OR), but this algorithm does not fail
			Assertions
					.assertThat(FilterHelpers.isStricterThan(notA_and_notB,
							ColumnFilter.matchEq("k" + 0, "v" + 0).negate()))
					.describedAs("nbNegated == %s", nbNegated)
					.isTrue();
		}
	}

	@Test
	public void testIsStricterThan_doubleNegation() {
		ISliceFilter equalsTo = ColumnFilter.matchEq("k", "v");
		ISliceFilter in = ColumnFilter.matchIn("k", "v", "v2");

		Assertions.assertThat(FilterHelpers.isStricterThan(equalsTo, in)).isTrue();
		Assertions.assertThat(FilterHelpers.isStricterThan(in, equalsTo)).isFalse();

		Assertions.assertThat(FilterHelpers.isStricterThan(NotFilter.builder().negated(equalsTo).build(),
				NotFilter.builder().negated(in).build())).isFalse();
		Assertions.assertThat(FilterHelpers.isStricterThan(NotFilter.builder().negated(in).build(),
				NotFilter.builder().negated(equalsTo).build())).isTrue();
	}

	@Test
	public void testIsStricterThan_comparison() {
		ISliceFilter equalsTo = ColumnFilter.matchEq("a", 123);
		ISliceFilter greaterThanOrOther = FilterBuilder
				.or(ColumnFilter.match("a", ComparingMatcher.strictlyGreaterThan(12)), ColumnFilter.matchEq("b", "b1"))
				.combine();

		Assertions.assertThat(FilterHelpers.isStricterThan(equalsTo, greaterThanOrOther)).isTrue();
		Assertions.assertThat(FilterHelpers.isStricterThan(greaterThanOrOther, equalsTo)).isFalse();
	}

	@Test
	public void testIsStricterThan_andIn() {
		ISliceFilter a_b =
				FilterBuilder.and(ColumnFilter.matchIn("a", "a1", "a2"), ColumnFilter.matchIn("b", "b1", "b2"))
						.optimize();
		ISliceFilter abc = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1", "c", "c1"));

		Assertions.assertThat(FilterHelpers.isStricterThan(abc, a_b)).isTrue();
	}

	@Test
	public void testIsLaxerThan() {
		Assertions.assertThat(FilterHelpers.isLaxerThan(ColumnFilter.matchIn("a", "a1", "a2", "a3"),
				ColumnFilter.matchIn("a", "a1", "a2"))).isTrue();
	}

	@Test
	public void testCommonFilter_negated() {
		ISliceFilter notA1 = ColumnFilter.matchEq("a", "a1").negate();
		ISliceFilter notA1B2 = AndFilter.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b2")).negate();

		Assertions.assertThat(FilterHelpers.commonAnd(Set.of(notA1, notA1B2))).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testSplitOr_or() {
		Set<ISliceFilter> splitted = FilterHelpers.splitOr(
				FilterBuilder
						.or(ColumnFilter.match("a", EqualsMatcher.matchEq("a1")),
								ColumnFilter.match("b", EqualsMatcher.matchEq("b2")))
						.combine());

		Assertions.assertThat(splitted)
				.contains(ColumnFilter.match("a", EqualsMatcher.matchEq("a1")))
				.contains(ColumnFilter.match("b", EqualsMatcher.matchEq("b2")))
				.hasSize(2);
	}

	// @Test
	// public void testSplitOr_notIn() {
	// // !(c==c1&b=in=(b1,b2))
	//
	// Set<ISliceFilter> splitted = FilterHelpers.splitOr(NotFilter.not(
	// FilterBuilder.and(ColumnFilter.isEqualTo("c", "c1"), ColumnFilter.isIn("b", "b1", "b2")).combine()));
	//
	// Assertions.assertThat(splitted)
	// .hasSize(3)
	// .containsExactly(ColumnFilter.isDistinctFrom("c", "c1"),
	// ColumnFilter.isDistinctFrom("b", "b1"),
	// ColumnFilter.isDistinctFrom("b", "b2"));
	//
	// }

	@Test
	public void testSplitAnd_notIn() {
		// c==c1&!(b=in=(b1,b2))

		Set<ISliceFilter> splitted = FilterHelpers.splitAnd(
				FilterBuilder.and(ColumnFilter.matchEq("c", "c1"), ColumnFilter.matchIn("b", "b1", "b2").negate())
						.combine());

		Assertions.assertThat(splitted)
				.hasSize(3)
				.containsExactly(ColumnFilter.matchEq("c", "c1"),
						ColumnFilter.notEq("b", "b1"),
						ColumnFilter.notEq("b", "b2"));

	}

	final IFilterOptimizer forbidOptimizations = new IFilterOptimizer() {

		@Override
		public ISliceFilter and(Collection<? extends ISliceFilter> filters, boolean willBeNegated) {
			throw new UnsupportedOperationException("Forbidden");
		}

		@Override
		public ISliceFilter or(Collection<? extends ISliceFilter> filters) {
			throw new UnsupportedOperationException("Forbidden");
		}

		@Override
		public ISliceFilter not(ISliceFilter first) {
			throw new UnsupportedOperationException("Forbidden");
		}

	};

	// Ensure `.splitAnd` must not do any optimization
	@Test
	public void testSplitAnd_checkNoOptimize() {
		AdhocFilterUnsafe.filterOptimizer = forbidOptimizations;

		try {

			Set<ISliceFilter> splitted =
					FilterHelpers.splitAnd(
							FilterBuilder
									.and(ColumnFilter.matchEq("c", "c1"),
											FilterBuilder.not(OrFilter.or(ImmutableMap.of("a", "a1", "b", "b1")))
													.combine())
									.combine());

			Assertions.assertThat(splitted)
					.hasSize(3)
					.containsExactly(ColumnFilter.matchEq("c", "c1"),
							ColumnFilter.notEq("a", "a1"),
							ColumnFilter.notEq("b", "b1"));
		} finally {
			AdhocFilterUnsafe.resetAll();
		}
	}

	// Ensure `.splitOr` must not do any optimization
	@Test
	public void testSplitOr_checkNoOptimize() {
		AdhocFilterUnsafe.filterOptimizer = forbidOptimizations;

		try {

			Set<ISliceFilter> splitted =
					FilterHelpers.splitOr(
							FilterBuilder
									.or(ColumnFilter.matchEq("c", "c1"),
											FilterBuilder.not(AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1")))
													.combine())
									.combine());

			Assertions.assertThat(splitted)
					.hasSize(3)
					.containsExactly(ColumnFilter.matchEq("c", "c1"),
							ColumnFilter.notEq("a", "a1"),
							ColumnFilter.notEq("b", "b1"));
		} finally {
			AdhocFilterUnsafe.resetAll();
		}
	}

	@Test
	public void testVisit() {
		IFilterVisitor visitor = new IFilterVisitor() {

			@Override
			public boolean testOrOperands(Set<? extends ISliceFilter> operands) {
				return false;
			}

			@Override
			public boolean testNegatedOperand(ISliceFilter negated) {
				return false;
			}

			@Override
			public boolean testColumnOperand(IColumnFilter columnFilter) {
				return false;
			}

			@Override
			public boolean testAndOperands(Set<? extends ISliceFilter> operands) {
				return false;
			}
		};
		Assertions.assertThat(FilterHelpers.visit(ISliceFilter.MATCH_ALL, visitor)).isFalse();
		Assertions.assertThat(FilterHelpers.visit(ISliceFilter.MATCH_NONE, visitor)).isFalse();
		Assertions.assertThat(FilterHelpers.visit(ColumnFilter.notEq("a", "a1"), visitor)).isFalse();
	}

	@Test
	public void testAsMap_notEqualsMatcher_throws() {
		// A ColumnFilter with a non-EqualsMatcher cannot be turned into a Map
		Assertions.assertThatThrownBy(() -> FilterHelpers.asMap(ColumnFilter.matchIn("k", "v1", "v2")))
				.isInstanceOf(Exception.class);
	}

	@Test
	public void testAsMap_andWithNonEqualsMatchers_throws() {
		// AND of non-EqualsMatcher column filters cannot be turned into a Map
		ISliceFilter filter =
				AndFilter.and(ColumnFilter.matchIn("a", "a1", "a2"), ColumnFilter.matchIn("b", "b1", "b2"));
		Assertions.assertThatThrownBy(() -> FilterHelpers.asMap(filter)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testSplitOr_notAnd() {
		// NOT(AND(a=a1, b=b1)) should split into OR(NOT(a=a1), NOT(b=b1))
		ISliceFilter notAnd = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1")).negate();
		Set<ISliceFilter> splitted = FilterHelpers.splitOr(notAnd);
		Assertions.assertThat(splitted)
				.hasSize(2)
				.containsExactlyInAnyOrder(ColumnFilter.notEq("a", "a1"), ColumnFilter.notEq("b", "b1"));
	}

	@Test
	public void testSplitOr_columnWithOrMatcher() {
		// A ColumnFilter whose value matcher is an OrMatcher should split into individual column filters
		ISliceFilter filter = ColumnFilter.builder()
				.column("c")
				.valueMatcher(OrMatcher.or(EqualsMatcher.matchEq("v1"), EqualsMatcher.matchEq("v2")))
				.build();
		Set<ISliceFilter> splitted = FilterHelpers.splitOr(filter);
		Assertions.assertThat(splitted)
				.hasSize(2)
				.containsExactlyInAnyOrder(ColumnFilter.matchEq("c", "v1"), ColumnFilter.matchEq("c", "v2"));
	}

	@Test
	public void testSplitAnd_columnWithAndMatcher() {
		// A ColumnFilter whose value matcher is an AndMatcher splits into individual column filters.
		// Use copyOf to bypass the optimizer (which would simplify eq("v1")&eq("v2") to MATCH_NONE)
		ISliceFilter filter = ColumnFilter.builder()
				.column("c")
				.valueMatcher(AndMatcher.copyOf(List.of(EqualsMatcher.matchEq("v1"), EqualsMatcher.matchEq("v2"))))
				.build();
		Set<ISliceFilter> splitted = FilterHelpers.splitAnd(filter);
		Assertions.assertThat(splitted)
				.hasSize(2)
				.containsExactlyInAnyOrder(ColumnFilter.matchEq("c", "v1"), ColumnFilter.matchEq("c", "v2"));
	}

	@Test
	public void testSplitAnd_columnWithNotInMatcher() {
		// A ColumnFilter with NOT(IN(v1, v2)) splits into NOT(=v1) AND NOT(=v2)
		ISliceFilter filter =
				ColumnFilter.builder().column("c").valueMatcher(NotMatcher.not(InMatcher.matchIn("v1", "v2"))).build();
		Set<ISliceFilter> splitted = FilterHelpers.splitAnd(filter);
		Assertions.assertThat(splitted)
				.hasSize(2)
				.containsExactlyInAnyOrder(ColumnFilter.notEq("c", "v1"), ColumnFilter.notEq("c", "v2"));
	}

	@Test
	public void testSplitAnd_noSplitMatchers() {
		// When splitMatchers=false, column filters with AndMatcher are NOT split
		ISliceFilter filter = ColumnFilter.builder()
				.column("c")
				.valueMatcher(AndMatcher.and(EqualsMatcher.matchEq("v1"), EqualsMatcher.matchEq("v2")))
				.build();
		Set<ISliceFilter> splitted = FilterHelpers.splitAnd(List.of(filter), false);
		Assertions.assertThat(splitted).containsExactly(filter);
	}

	@Test
	public void testGetFilteredColumns_matchAll_empty() {
		// MATCH_ALL is an empty AND filter; its filtered columns set is empty
		Assertions.assertThat(FilterHelpers.getFilteredColumns(ISliceFilter.MATCH_ALL)).isEmpty();
	}

	@Test
	public void testGetFilteredColumns_column() {
		// Single column filter has exactly that column in the filtered columns set
		Assertions.assertThat(FilterHelpers.getFilteredColumns(ColumnFilter.matchEq("myCol", "v")))
				.containsExactly("myCol");
	}

	@Test
	public void testSimplifyOrGivenContribution() {
		// contribution|output=filter → strip contribution.negate() from filter.negate()
		ISliceFilter contribution = ColumnFilter.matchEq("a", "a1");
		ISliceFilter filter =
				FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b1")).combine();

		ISliceFilter simplified = FilterHelpers.simplifyOrGivenContribution(contribution, filter);
		// After removing contribution=a=a1, should be left with b=b1
		Assertions.assertThat(FilterHelpers.isLaxerThan(simplified, ColumnFilter.matchEq("b", "b1"))).isTrue();
	}

	@Test
	public void testCommonOr() {
		ISliceFilter f1 = FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b1")).combine();
		ISliceFilter f2 = FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("c", "c1")).combine();

		// commonOr finds what is common across both OR filters: a=a1 is common
		ISliceFilter common = FilterHelpers.commonOr(com.google.common.collect.ImmutableSet.of(f1, f2));
		Assertions.assertThat(FilterHelpers.isStricterThan(ColumnFilter.matchEq("a", "a1"), common)).isTrue();
	}

}
