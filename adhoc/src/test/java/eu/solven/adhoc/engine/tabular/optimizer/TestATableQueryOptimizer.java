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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;

public class TestATableQueryOptimizer {

	Aggregator sumK1 = Aggregator.sum("k1");
	CubeQueryStep contextStep = CubeQueryStep.builder().measure(sumK1).build();

	CubeQueryStep groupByA = CubeQueryStep.builder().groupBy(GroupByColumns.named("a")).measure(sumK1).build();
	CubeQueryStep groupByB = CubeQueryStep.builder().groupBy(GroupByColumns.named("b")).measure(sumK1).build();
	CubeQueryStep filterA1 = CubeQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.measure(sumK1)
			.filter(AndFilter.and(Map.of("a", "a1")))
			.build();
	CubeQueryStep filterA1B1 = CubeQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.measure(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1")))
			.build();
	CubeQueryStep filterA12 = CubeQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.measure(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a", ImmutableSet.of("a1", "a2"))))
			.build();

	CubeQueryStep filterAPrefix = CubeQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.measure(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a", LikeMatcher.matching("a%"))))
			.build();

	CubeQueryStep filterAPrefixNotAzerty = CubeQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.measure(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a",
					AndMatcher.and(LikeMatcher.matching("a%"), NotMatcher.not(EqualsMatcher.matchEq("azerty"))))))
			.build();

	Aggregator sumK2 = Aggregator.sum("k2");
	CubeQueryStep groupByA_K2 = CubeQueryStep.builder().groupBy(GroupByColumns.named("a")).measure(sumK2).build();

	ATableQueryOptimizer optimizer = new ATableQueryOptimizer(AdhocFactoriesUnsafe.factories) {

		@Override
		public SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<CubeQueryStep> querySteps) {
			throw new UnsupportedOperationException();
		}
	};

	@Test
	public void testGrandTotalAndFilterA1() {
		TableQueryV3 v2 = optimizer.makeTableQuery(contextStep, ImmutableSet.of(groupByA, filterA1));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV3.builder()
						.groupBy(GroupByColumns.named("a"))
						.aggregator(
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and(Map.of())).build())
						.aggregator(FilteredAggregator.builder()
								.aggregator(sumK1)
								.filter(AndFilter.and(Map.of("a", "a1")))
								.index(1)
								.build())
						.build());
	}

	@Test
	public void testFilterA1FilterA1B1() {
		TableQueryV3 v2 = optimizer.makeTableQuery(contextStep, ImmutableSet.of(filterA1, filterA1B1));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV3.builder()
						.groupBy(GroupByColumns.named("a"))
						.aggregator(
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and(Map.of())).build())
						.aggregator(FilteredAggregator.builder()
								.aggregator(sumK1)
								.filter(AndFilter.and(Map.of("b", "b1")))
								.index(1)
								.build())
						.filter(AndFilter.and(Map.of("a", "a1")))
						.build());
	}

	// IN is not AND
	@Test
	public void testFilterA1FilterA12() {
		TableQueryV3 v2 = optimizer.makeTableQuery(contextStep, ImmutableSet.of(filterA1, filterA12));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV3.builder()
						.groupBy(GroupByColumns.named("a"))
						.aggregator(FilteredAggregator.builder()
								.aggregator(sumK1)
								.filter(AndFilter.and(Map.of("a", "a1")))
								.build())
						.aggregator(FilteredAggregator.builder()
								.aggregator(sumK1)
								.filter(AndFilter.and(Map.of("a", Set.of("a1", "a2"))))
								.index(1)
								.build())
						// .filter(AndFilter.and(Map.of("a", "a1")))
						.build());
	}

	@Test
	public void testAndMatcher() {
		TableQueryV3 v2 = optimizer.makeTableQuery(contextStep, ImmutableSet.of(filterAPrefix, filterAPrefixNotAzerty));
		Assertions.assertThat(v2).satisfies(q -> {
			Assertions.assertThat(q.getGroupBys()).containsExactly(GroupByColumns.named("a"));
			Assertions.assertThat(q.getFilter()).isEqualTo(ColumnFilter.matchLike("a", "a%"));
			Assertions.assertThat(q.getAggregators())
					.hasSize(2)
					.contains(FilteredAggregator.builder().aggregator(sumK1).filter(ISliceFilter.MATCH_ALL).build())
					.contains(FilteredAggregator.builder()
							.aggregator(sumK1)
							.filter(AndFilter.and(Map.of("a", NotMatcher.not(EqualsMatcher.matchEq("azerty")))))
							.index(1)
							.build());
		});
	}

	@Test
	public void testDifferentAggregators() {
		TableQueryV3 v2 = optimizer.makeTableQuery(contextStep, ImmutableSet.of(groupByA, groupByA_K2));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV3.builder()
						.groupBy(GroupByColumns.named("a"))
						.aggregator(
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and(Map.of())).build())
						.aggregator(
								FilteredAggregator.builder().aggregator(sumK2).filter(AndFilter.and(Map.of())).build())
						.build());
	}

	@Test
	public void testNoMeasure() {
		TableQueryV3 v2 =
				optimizer.makeTableQuery(CubeQueryStep.builder().measure(EmptyMeasure.builder().build()).build(),
						ImmutableSet.of(CubeQueryStep.builder()
								.filter(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2")))
								.measure(Aggregator.empty())
								.build()));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV3.builder()
						.groupBy(IGroupBy.GRAND_TOTAL)
						.filter(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2")))
						.aggregator(FilteredAggregator.builder()
								.aggregator(Aggregator.empty())
								.filter(ISliceFilter.MATCH_ALL)
								.build())
						.build());
	}

	@Test
	public void testGroupBy_independantColumn_sameMeasure() {
		TableQueryV3 v2 = optimizer.makeTableQuery(contextStep, ImmutableSet.of(groupByA, groupByB));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV3.builder()
						.groupBy(GroupByColumns.named("a"))
						.groupBy(GroupByColumns.named("b"))
						.aggregator(FilteredAggregator.builder().aggregator(sumK1).build())
						.build());
	}

	@Test
	public void testBreakSorting() {
		Assertions.assertThat(optimizer.breakSorting(ImmutableSortedSet.of("a"), ImmutableSortedSet.of("a"))).isFalse();
		Assertions.assertThat(optimizer.breakSorting(ImmutableSortedSet.of("a"), ImmutableSortedSet.of())).isFalse();
		Assertions.assertThat(optimizer.breakSorting(ImmutableSortedSet.of("a"), ImmutableSortedSet.of("a", "b")))
				.isTrue();

		Assertions.assertThat(optimizer.breakSorting(ImmutableSortedSet.of("a", "b"), ImmutableSortedSet.of("a")))
				.isFalse();
		Assertions.assertThat(optimizer.breakSorting(ImmutableSortedSet.of("a", "b"), ImmutableSortedSet.of()))
				.isFalse();
		Assertions.assertThat(optimizer.breakSorting(ImmutableSortedSet.of("a", "b"), ImmutableSortedSet.of("b")))
				.isTrue();
		Assertions.assertThat(optimizer.breakSorting(ImmutableSortedSet.of("a", "b"), ImmutableSortedSet.of("a", "c")))
				.isTrue();
	}
}
