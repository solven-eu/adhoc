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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.splitter.InduceByAdhoc;
import eu.solven.adhoc.engine.tabular.splitter.InduceByAdhocComplete;
import eu.solven.adhoc.engine.tabular.splitter.merger.MergeInducersIntoSingle;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.IFilterQueryBundle;
import eu.solven.adhoc.filter.OrFilter;
import eu.solven.adhoc.filter.optimizer.FilterOptimizer;
import eu.solven.adhoc.filter.optimizer.FilterOptimizerWithCache;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizerFactory;
import eu.solven.adhoc.filter.stripper.FilterStripperFactory;
import eu.solven.adhoc.filter.stripper.FilterStripperUnsafe;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IHasQueryOptionsAndExecutorService;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;

public class TestInduceByAdhocMergingIntoSingle_groupByAggregator implements IAdhocTestConstants {
	FilterStripperFactory stripperFactory = FilterStripperFactory.builder().build();
	FilterOptimizer filterOptimizer = FilterOptimizerWithCache.builder().filterStripperFactory(stripperFactory).build();

	TableQueryStep step = TableQueryStep.builder().aggregator(k1Sum).build();
	AdhocFactories factories = AdhocFactories.builder()
			.filterStripperFactory(stripperFactory)
			.filterOptimizerFactory(IFilterOptimizerFactory.standard())
			.build();

	// Shared bundle ensures a single makeFilterStripper(MATCH_ALL) call across TableQueryFactory and InduceByAdhoc
	IFilterQueryBundle sharedBundle = factories.makeQueryBundle();

	TableQueryFactory optimizer = TableQueryFactory.builder()
			.filterBundle(sharedBundle)
			.splitter(InduceByAdhoc.builder()
					.filterBundle(sharedBundle)
					.mergeInducersFactory(MergeInducersIntoSingle.makeFactory())
					.inferenceEdgesAdderFactory((IFilterQueryBundle filterBundle) -> InduceByAdhocComplete.builder()
							.filterStripperFactory(filterBundle.getFilterStripperFactory())
							.build())
					.build())
			.groupByAggregator()
			.build();

	@Test
	public void testCanInduce_OrDifferentColumns() {
		TableQuery tq1 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("a", "a1"))
				.groupBy(GroupByColumns.named("b"))

				.build();
		TableQuery tq2 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("c", "c1"))
				.groupBy(GroupByColumns.named("d"))

				.build();
		SplitTableQueries split = optimizer.splitInducedLegacy(IHasQueryOptionsAndExecutorService.noOption(), ImmutableSet.of(tq1, tq2));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(TableQueryStep.edit(step)
						.filter(FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("c", "c1"))
								.optimize(filterOptimizer))
						.groupBy(GroupByColumns.named("a", "b", "c", "d"))
						.build());

		Assertions.assertThat(split.getInduceds())
				.hasSize(2)
				.contains(TableQueryStep.edit(step)
						.filter(ColumnFilter.matchEq("a", "a1"))
						.groupBy(GroupByColumns.named("b"))
						.build())
				.contains(TableQueryStep.edit(step)
						.filter(ColumnFilter.matchEq("c", "c1"))
						.groupBy(GroupByColumns.named("d"))
						.build());

		Assertions.assertThat(FilterStripperUnsafe.getNbMake(stripperFactory)).isEqualTo(1);
	}

	@Test
	public void testCanInduce_OrDifferentColumns_noMeasure() {
		TableQuery tq1 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("a", "a1"))
				.groupBy(GroupByColumns.named("b"))
				.clearAggregators()
				.build();
		TableQuery tq2 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("c", "c1"))
				.groupBy(GroupByColumns.named("d"))
				.clearAggregators()
				.build();
		SplitTableQueries split = optimizer.splitInducedLegacy(IHasQueryOptionsAndExecutorService.noOption(), Set.of(tq1, tq2));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(TableQueryStep.edit(step)
						.filter(FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("c", "c1"))
								.optimize(filterOptimizer))
						.groupBy(GroupByColumns.named("a", "b", "c", "d"))
						.aggregator(Aggregator.empty())
						.build());

		Assertions.assertThat(split.getInduceds())
				.hasSize(2)
				.contains(TableQueryStep.edit(step)
						.filter(ColumnFilter.matchEq("a", "a1"))
						.groupBy(GroupByColumns.named("b"))
						.aggregator(Aggregator.empty())
						.build())
				.contains(TableQueryStep.edit(step)
						.filter(ColumnFilter.matchEq("c", "c1"))
						.groupBy(GroupByColumns.named("d"))
						.aggregator(Aggregator.empty())
						.build());
	}

	@Test
	public void testCanInduce_AndDifferentColumns() {
		TableQuery tq1 = TableQuery.edit(step)
				.filter(AndFilter.and("a", "a1", "b", "b1"))
				.groupBy(GroupByColumns.named("b"))

				.build();
		TableQuery tq2 = TableQuery.edit(step)
				.filter(AndFilter.and("a", "a1", "c", "c1"))
				.groupBy(GroupByColumns.named("d"))

				.build();
		SplitTableQueries split = optimizer.splitInducedLegacy(IHasQueryOptionsAndExecutorService.noOption(), Set.of(tq1, tq2));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(
						TableQueryStep.edit(step)
								.filter(FilterBuilder
										.and(AndFilter.and("a", "a1"),
												FilterBuilder
														.or(ColumnFilter.matchEq("b", "b1"),
																ColumnFilter.matchEq("c", "c1"))
														.combine())
										.combine())
								.groupBy(GroupByColumns.named("b", "c", "d"))
								.build());

		Assertions.assertThat(split.getInduceds())
				.hasSize(2)
				.contains(TableQueryStep.edit(step)
						.filter(AndFilter.and("a", "a1", "b", "b1"))
						.groupBy(GroupByColumns.named("b"))
						.build())
				.contains(TableQueryStep.edit(step)
						.filter(AndFilter.and("a", "a1", "c", "c1"))
						.groupBy(GroupByColumns.named("d"))
						.build());
	}

	@Test
	public void testCanInduce_AndDifferentColumns_andThirdOnlyCommonFilter() {
		TableQuery tq1 = TableQuery.edit(step)
				.filter(AndFilter.and("a", "a1", "b", "b1"))
				.groupBy(GroupByColumns.named("b"))

				.build();
		TableQuery tq2 = TableQuery.edit(step)
				.filter(AndFilter.and("a", "a1", "c", "c1"))
				.groupBy(GroupByColumns.named("d"))

				.build();
		TableQuery tq3 = TableQuery.edit(step).filter(AndFilter.and("a", "a1")).build();
		SplitTableQueries split = optimizer.splitInducedLegacy(IHasQueryOptionsAndExecutorService.noOption(), Set.of(tq1, tq2, tq3));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(TableQueryStep.edit(step)
						.filter(AndFilter.and("a", "a1"))
						.groupBy(GroupByColumns.named("b", "c", "d"))
						.build());

		Assertions.assertThat(split.getInduceds())
				.hasSize(3)
				.contains(TableQueryStep.edit(step)
						.filter(AndFilter.and("a", "a1", "b", "b1"))
						.groupBy(GroupByColumns.named("b"))
						.build())
				.contains(TableQueryStep.edit(step)
						.filter(AndFilter.and("a", "a1", "c", "c1"))
						.groupBy(GroupByColumns.named("d"))
						.build())
				.contains(TableQueryStep.edit(step).filter(ColumnFilter.matchEq("a", "a1")).build());
	}

	@Test
	public void testCanInduce_SelfAndGranular() {
		TableQuery tq1 = TableQuery.edit(step).groupBy(GroupByColumns.named("a", "b")).build();
		TableQuery tq2 = TableQuery.edit(step).groupBy(GroupByColumns.named("a")).build();
		SplitTableQueries split = optimizer.splitInducedLegacy(IHasQueryOptionsAndExecutorService.noOption(), Set.of(tq1, tq2));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(TableQueryStep.edit(step).groupBy(GroupByColumns.named("a", "b")).build());

		Assertions.assertThat(split.getInduceds())
				.hasSize(1)
				.contains(TableQueryStep.edit(step).groupBy(GroupByColumns.named("a")).build());
	}

	@Test
	public void testCanInduce_chainOfInducers() {
		TableQueryStep tqAC = TableQueryStep.edit(step).groupBy(GroupByColumns.named("a", "c")).build();
		TableQueryStep tqAB = TableQueryStep.edit(step).groupBy(GroupByColumns.named("a", "b")).build();
		TableQueryStep tqA = TableQueryStep.edit(step).groupBy(GroupByColumns.named("a")).build();
		SplitTableQueries split = optimizer.splitInduced(IHasQueryOptionsAndExecutorService.noOption(), Set.of(tqAC, tqAB, tqA));

		Assertions.assertThat(split.getInducedToInducer().vertexSet()).hasSize(4);

		TableQueryStep merged = TableQueryStep.edit(step).groupBy(GroupByColumns.named("a", "b", "c")).build();
		Assertions.assertThat(split.getInducers()).hasSize(1).contains(merged);

		Assertions.assertThat(split.getInduceds())
				.hasSize(3)
				.contains(TableQueryStep.edit(step).groupBy(GroupByColumns.named("a")).build())
				.contains(TableQueryStep.edit(step).groupBy(GroupByColumns.named("a", "b")).build())
				.contains(TableQueryStep.edit(step).groupBy(GroupByColumns.named("a", "c")).build());

		Assertions.assertThat(split.getInducedToInducer().edgeSet())
				// `a,b,c->a,b`, `a,b,c->a,c`, `a,c->a`
				.hasSize(4)
				.anySatisfy(GraphsTestHelpers.assertEdge(tqAB, merged, split))
				.anySatisfy(GraphsTestHelpers.assertEdge(tqAC, merged, split))
				.anySatisfy(GraphsTestHelpers.assertEdge(tqA, tqAB, split))
				.anySatisfy(GraphsTestHelpers.assertEdge(tqA, tqAC, split));
	}

	/**
	 * Given `GROUP BY a,b FILTER d`, `GROUP BY a,c FILTER d` and `GROUP BY a FILTER e`, we may generate a single
	 * inducer `GROUP BY a,b,c FILTER d || e`. In the case `FILTER d` is empty, it is a pity to execute the filter
	 * twice, given there is 1 steps with `FILTER d`.
	 * 
	 * hence, we ensure we add intermediate steps per `FILTER`, in order to share FILTER.
	 */
	@Test
	public void testCanInduce_IntermediateNodeForFilterSharing() {
		TableQueryStep tq1 = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("a", "b"))
				.filter(ColumnFilter.matchEq("d", "d1"))

				.build();
		TableQueryStep tq2 = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("a", "c"))
				.filter(ColumnFilter.matchEq("d", "d1"))

				.build();
		TableQueryStep tq3 = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("a"))
				.filter(ColumnFilter.matchEq("e", "e1"))

				.build();
		SplitTableQueries split = optimizer.splitInduced(IHasQueryOptionsAndExecutorService.noOption(), Set.of(tq1, tq2, tq3));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(TableQueryStep.edit(step)
						.filter(FilterBuilder.or(ColumnFilter.matchEq("d", "d1"), ColumnFilter.matchEq("e", "e1"))
								.optimize(filterOptimizer))
						.groupBy(GroupByColumns.named("a", "b", "c", "d", "e"))
						.build());

		Assertions.assertThat(split.getInduceds())
				.hasSize(4)
				.contains(tq1, tq2, tq3)
				// intermediate for tq1 and tq2
				.contains(TableQueryStep.edit(step)
						.filter(ColumnFilter.matchEq("d", "d1"))
						.groupBy(GroupByColumns.named("a", "b", "c"))
						.build());
	}

	// `a&b|a&b|a&b&c|a&c|d`
	@Test
	public void testCanInduce_IntermediateNodeForFilterSharing_deep() {
		TableQueryStep tq1 = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("x"))
				.filter(AndFilter.and("a", "a1", "b", "b1"))

				.build();
		// second step has same filter with different groupBy
		TableQueryStep tq2 = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("y"))
				.filter(AndFilter.and("a", "a1", "b", "b1"))

				.build();
		// third step has stricter filter
		TableQueryStep tq3 = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("x"))
				.filter(AndFilter.and("a", "a1", "b", "b1", "c", "c1"))

				.build();
		// fourth step has intermediate filter: it may lead to ordering issues
		TableQueryStep tq4 = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("x"))
				.filter(AndFilter.and("a", "a1", "c", "c1"))

				.build();
		// fifth step has unrelated filter
		TableQueryStep tq5 = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("x"))
				.filter(AndFilter.and("d", "d1"))

				.build();
		SplitTableQueries split = optimizer.splitInduced(IHasQueryOptionsAndExecutorService.noOption(), Set.of(tq1, tq2, tq3, tq4, tq5));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(TableQueryStep.edit(step)
						.filter(FilterBuilder
								.or(ColumnFilter.matchEq("d", "d1"),
										AndFilter.and(ColumnFilter.matchEq("a", "a1"),
												OrFilter.or("c", "c1", "b", "b1")))
								.combine())
						.groupBy(GroupByColumns.named("a", "b", "c", "d", "x", "y"))
						.build());

		Assertions.assertThat(split.getInduceds())
				.hasSize(7)
				.contains(tq1, tq2, tq3, tq4, tq5)

				// Shared for the whole `a=a1` branch
				// `d=d1` is present in the filter as it comes from the merged inducer
				// .contains(TableQueryStep.edit(step)
				// .filter(FilterBuilder
				// .and(ColumnFilter.matchEq("a", "a1"),
				// OrFilter.or(ImmutableMap.of("b", "b1", "c", "c1")))
				// .combine())
				// .groupBy(GroupByColumns.named("b", "c", "x", "y"))
				// .build())

				// intermediate to the `a|b|c` branch
				.contains(TableQueryStep.edit(step)
						.filter(FilterBuilder.and(ColumnFilter.matchEq("a", "a1"), OrFilter.or("b", "b1", "c", "c1"))
								.combine())
						// `a` is not groupedBy as it is common to all
						.groupBy(GroupByColumns.named("b", "c", "x", "y"))
						.build())

				// intermediate for the 2 `a|b` steps
				.contains(TableQueryStep.edit(step)
						.filter(AndFilter.and("a", "a1", "b", "b1"))
						// BEWARE `c` is present as groupBy as this intermediate is also used for `a&b&c` step
						.groupBy(GroupByColumns.named("c", "x", "y"))
						.build());
	}

	// The covers a case where `GROUP BY (a,b)` is very wide but `(a1)` and `(b1)` has only one slice `(a1,b1)`,
	// highlighting cases where we must not do `GROUP BY (a,b)`
	@Test
	@Disabled("May be relevant for an alternative splitter")
	public void testSplit_a1GroupByB_b1GroupByA() {
		TableQueryStep b1GroupA = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("a"))
				.filter(ColumnFilter.matchEq("b", "b1"))

				.build();
		TableQueryStep a1GroupB = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("b"))
				.filter(ColumnFilter.matchEq("a", "a1"))

				.build();
		SplitTableQueries split = optimizer.splitInduced(IHasQueryOptionsAndExecutorService.noOption(), Set.of(b1GroupA, a1GroupB));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(TableQueryStep.edit(step).groupBy(GroupByColumns.named("a", "b")).build());

		Assertions.assertThat(split.getInduceds()).hasSize(2).contains(b1GroupA, a1GroupB);
	}

	@Test
	public void testSplit_a1GroupByA_a2GroupByA() {
		TableQueryStep a1GroupA = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("a"))
				.filter(ColumnFilter.matchEq("a", "a1"))

				.build();
		TableQueryStep a2GroupA = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("a"))
				.filter(ColumnFilter.matchEq("a", "a2"))

				.build();
		SplitTableQueries split = optimizer.splitInduced(IHasQueryOptionsAndExecutorService.noOption(), Set.of(a1GroupA, a2GroupA));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(TableQueryStep.edit(step)
						.groupBy(GroupByColumns.named("a"))
						.filter(ColumnFilter.matchIn("a", "a1", "a2"))
						.build());

		Assertions.assertThat(split.getInduceds()).hasSize(2).contains(a1GroupA, a2GroupA);
	}

	@Test
	public void testSplit_a1GroupByAB_GroupByA() {
		TableQueryStep a1GroupA = TableQueryStep.edit(step)
				.groupBy(GroupByColumns.named("a", "b"))
				.filter(ColumnFilter.matchEq("a", "a1"))

				.build();
		TableQueryStep a2GroupA = TableQueryStep.edit(step).groupBy(GroupByColumns.named("a")).build();
		SplitTableQueries split = optimizer.splitInduced(IHasQueryOptionsAndExecutorService.noOption(), Set.of(a1GroupA, a2GroupA));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(TableQueryStep.edit(step).groupBy(GroupByColumns.named("a", "b")).build());

		Assertions.assertThat(split.getInduceds()).hasSize(2).contains(a1GroupA, a2GroupA);
	}
}
