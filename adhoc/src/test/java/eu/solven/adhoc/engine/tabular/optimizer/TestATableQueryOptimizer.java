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
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.AndMatcher;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.LikeMatcher;
import eu.solven.adhoc.filter.value.NotMatcher;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.options.IHasQueryOptionsAndExecutorService;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;

public class TestATableQueryOptimizer {

	Aggregator sumK1 = Aggregator.sum("k1");
	TableQueryStep contextStep = TableQueryStep.builder().aggregator(sumK1).build();

	TableQueryStep groupByA = TableQueryStep.builder().groupBy(GroupByColumns.named("a")).aggregator(sumK1).build();
	TableQueryStep groupByB = TableQueryStep.builder().groupBy(GroupByColumns.named("b")).aggregator(sumK1).build();
	TableQueryStep filterA1 = TableQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and("a", "a1"))
			.build();
	TableQueryStep filterA1B1 = TableQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and("a", "a1", "b", "b1"))
			.build();
	TableQueryStep filterA12 = TableQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a", ImmutableSet.of("a1", "a2"))))
			.build();

	TableQueryStep filterAPrefix = TableQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a", LikeMatcher.matching("a%"))))
			.build();

	TableQueryStep filterAPrefixNotAzerty = TableQueryStep.builder()
			.groupBy(GroupByColumns.named("a"))
			.aggregator(sumK1)
			.filter(AndFilter.and(ImmutableMap.of("a",
					AndMatcher.and(LikeMatcher.matching("a%"), NotMatcher.not(EqualsMatcher.matchEq("azerty"))))))
			.build();

	Aggregator sumK2 = Aggregator.sum("k2");
	TableQueryStep groupByA_K2 = TableQueryStep.builder().groupBy(GroupByColumns.named("a")).aggregator(sumK2).build();

	ATableQueryFactory optimizer = new ATableQueryFactory(AdhocFactoriesUnsafe.factories.makeQueryBundle()) {

		@Override
		public SplitTableQueries splitInduced(IHasQueryOptionsAndExecutorService hasOptions, Set<TableQueryStep> querySteps) {
			throw new UnsupportedOperationException();
		}
	};

	@Test
	public void testGrandTotalAndFilterA1() {
		TableQueryV4 v2 = optimizer.makeTableQueryV4(contextStep, ImmutableSet.of(groupByA, filterA1));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV4.builder()
						.groupByToAggregator(GroupByColumns.named("a"),
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and(Map.of())).build())
						.groupByToAggregator(GroupByColumns.named("a"),
								FilteredAggregator.builder()
										.aggregator(sumK1)
										.filter(AndFilter.and("a", "a1"))
										.index(1)
										.build())
						.build());
	}

	@Test
	public void testFilterA1FilterA1B1() {
		TableQueryV4 v2 = optimizer.makeTableQueryV4(contextStep, ImmutableSet.of(filterA1, filterA1B1));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV4.builder()
						.groupByToAggregator(GroupByColumns.named("a"),
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and(Map.of())).build())
						.groupByToAggregator(GroupByColumns.named("a"),
								FilteredAggregator.builder()
										.aggregator(sumK1)
										.filter(AndFilter.and("b", "b1"))
										.index(1)
										.build())
						.filter(AndFilter.and("a", "a1"))
						.build());
	}

	// IN is not AND
	@Test
	public void testFilterA1FilterA12() {
		TableQueryV4 v2 = optimizer.makeTableQueryV4(contextStep, ImmutableSet.of(filterA1, filterA12));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV4.builder()
						.groupByToAggregator(GroupByColumns.named("a"),
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and("a", "a1")).build())
						.groupByToAggregator(GroupByColumns.named("a"),
								FilteredAggregator.builder()
										.aggregator(sumK1)
										.filter(AndFilter.and("a", Set.of("a1", "a2")))
										.index(1)
										.build())
						// .filter(AndFilter.and(Map.of("a", "a1")))
						.build());
	}

	@Test
	public void testAndMatcher() {
		TableQueryV4 v2 =
				optimizer.makeTableQueryV4(contextStep, ImmutableSet.of(filterAPrefix, filterAPrefixNotAzerty));
		Assertions.assertThat(v2).satisfies(q -> {
			Assertions.assertThat(q.getFilter()).isEqualTo(ColumnFilter.matchLike("a", "a%"));
			Assertions.assertThat(v2.getGroupByToAggregators().asMap())
					.hasSize(1)
					.containsKey(GroupByColumns.named("a"))

					.extractingByKey(GroupByColumns.named("a"), InstanceOfAssertFactories.SET)
					.hasSize(2)
					.contains(FilteredAggregator.builder().aggregator(sumK1).filter(ISliceFilter.MATCH_ALL).build())
					.contains(FilteredAggregator.builder()
							.aggregator(sumK1)
							.filter(AndFilter.and("a", NotMatcher.not(EqualsMatcher.matchEq("azerty"))))
							.index(1)
							.build());
		});
	}

	@Test
	public void testDifferentAggregators() {
		TableQueryV4 v2 = optimizer.makeTableQueryV4(contextStep, ImmutableSet.of(groupByA, groupByA_K2));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV4.builder()
						.groupByToAggregator(GroupByColumns.named("a"),
								FilteredAggregator.builder().aggregator(sumK1).filter(AndFilter.and(Map.of())).build())
						.groupByToAggregator(GroupByColumns.named("a"),
								FilteredAggregator.builder().aggregator(sumK2).filter(AndFilter.and(Map.of())).build())
						.build());
	}

	@Test
	public void testNoMeasure() {
		TableQueryV4 v2 =
				optimizer.makeTableQueryV4(CubeQueryStep.builder().measure(EmptyMeasure.builder().build()).build(),
						ImmutableSet.of(TableQueryStep.builder()
								.filter(AndFilter.and("k1", "v1", "k2", "v2"))
								.aggregator(Aggregator.empty())
								.build()));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV4.builder()
						.filter(AndFilter.and("k1", "v1", "k2", "v2"))
						.groupByToAggregator(IGroupBy.GRAND_TOTAL,
								FilteredAggregator.builder()
										.aggregator(Aggregator.empty())
										.filter(ISliceFilter.MATCH_ALL)
										.build())
						.build());
	}

	@Test
	public void testGroupBy_independantColumn_sameMeasure() {
		TableQueryV4 v2 = optimizer.makeTableQueryV4(contextStep, ImmutableSet.of(groupByA, groupByB));
		Assertions.assertThat(v2)
				.isEqualTo(TableQueryV4.builder()
						.groupByToAggregator(GroupByColumns.named("a"),
								FilteredAggregator.builder().aggregator(sumK1).build())
						.groupByToAggregator(GroupByColumns.named("b"),
								FilteredAggregator.builder().aggregator(sumK1).build())
						.build());
	}
}
