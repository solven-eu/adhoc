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
package eu.solven.adhoc.engine.tabular.splitter;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowingConsumer;
import org.jgrapht.graph.DefaultEdge;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphHelpers;
import eu.solven.adhoc.engine.tabular.optimizer.IAdhocDag;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.OrFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestInduceByAdhocOptimistic {
	Aggregator m1 = Aggregator.sum("m1");
	Aggregator m2 = Aggregator.sum("m2");

	TableQueryStep a = TableQueryStep.builder().aggregator(m1).build();

	AInduceByAdhocParent splitter = new InduceByAdhocComplete();

	@Test
	public void testCanInduce_Trivial() {
		// Different measure by reference
		Assertions.assertThat(splitter.canInduce(a, a.toBuilder().aggregator(m2).build())).isFalse();
		// Different measure with same name
		Assertions.assertThat(splitter.canInduce(a, a.toBuilder().aggregator(m1).build())).isTrue();

		// Less columns
		Assertions.assertThat(splitter.canInduce(a.toBuilder().groupBy(GroupByColumns.named("g", "h")).build(),
				a.toBuilder().groupBy(GroupByColumns.named("g")).build())).isTrue();
		// More columns
		Assertions.assertThat(splitter.canInduce(a.toBuilder().groupBy(GroupByColumns.named("g", "h")).build(),
				a.toBuilder().groupBy(GroupByColumns.named("g", "h", "i")).build())).isFalse();

		// Different column same coordinate
		Assertions.assertThat(splitter.canInduce(
				a.toBuilder().groupBy(GroupByColumns.named("g", "h")).filter(ColumnFilter.matchEq("c", "c1")).build(),
				a.toBuilder().filter(ColumnFilter.matchEq("d", "c1")).build())).isFalse();
	}

	@Test
	public void testCanInduce_OrDifferentColumns() {
		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				a.toBuilder()
						.filter(FilterBuilder.or(ColumnFilter.matchEq("c", "c1"), ColumnFilter.matchEq("d", "d1"))
								.optimize())
						.build(),
				// induced has only one of filters
				a.toBuilder().filter(ColumnFilter.matchEq("c", "c1")).build()))
				// false because filtered columns are not groupedBy
				.isFalse();

		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				a.toBuilder()
						.groupBy(GroupByColumns.named("g", "h"))
						.filter(FilterBuilder.or(ColumnFilter.matchEq("g", "g1"), ColumnFilter.matchEq("h", "h1"))
								.optimize())
						.build(),
				// induced has only one of filters
				a.toBuilder().filter(ColumnFilter.matchEq("g", "g1")).build()))
				// true because filtered columns are groupedBy: irrelevant `g` can be filtered.
				.isTrue();

		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				a.toBuilder()
						.groupBy(GroupByColumns.named("g", "h"))
						.filter(FilterBuilder.or(ColumnFilter.matchEq("g", "g1"), ColumnFilter.matchEq("c", "c1"))
								.optimize())
						.build(),
				// induced has only one of filters
				a.toBuilder().filter(ColumnFilter.matchEq("g", "g1")).build()))
				// true because inducer has more rows than induced, and these rows can be filtered out (based on `g`)
				.isTrue();
	}

	@Test
	public void testCanInduce_AndDifferentColumns() {
		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				a.toBuilder().filter(ColumnFilter.matchEq("c", "c1")).build(),
				// induced has only one of filters
				a.toBuilder()
						.filter(AndFilter.and(ColumnFilter.matchEq("c", "c1"), ColumnFilter.matchEq("d", "c1")))
						.build()))
				// false because filtered columns are not groupedBy
				.isFalse();

		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				a.toBuilder().groupBy(GroupByColumns.named("g", "h")).filter(ColumnFilter.matchEq("g", "c1")).build(),
				// induced has only one of filters
				a.toBuilder()
						.filter(AndFilter.and(ColumnFilter.matchEq("g", "c1"), ColumnFilter.matchEq("h", "c1")))
						.build()))
				// true because filtered columns are groupedBy
				.isTrue();

		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				a.toBuilder().filter(ColumnFilter.matchEq("g", "c1")).build(),
				// induced has only one of filters
				a.toBuilder()
						.filter(AndFilter.and(ColumnFilter.matchEq("g", "c1"), ColumnFilter.matchEq("c", "c1")))
						.build()))
				// false because inducer lacks information to filter along c
				.isFalse();
	}

	@Test
	public void testCanInduce_Same() {
		Assertions.assertThat(splitter.canInduce(
				a.toBuilder().filter(ColumnFilter.matchEq("c", "c1")).groupBy(GroupByColumns.named("d")).build(),
				a.toBuilder().filter(ColumnFilter.matchEq("c", "c1")).groupBy(GroupByColumns.named("d")).build()))
				.isTrue();
	}

	@Test
	public void testCanInduce_DifferentTopology() {
		Assertions.assertThat(splitter.canInduce(
				// groupBy (g,h) matchAll
				a.toBuilder().groupBy(GroupByColumns.named("g", "h")).filter(ISliceFilter.MATCH_ALL).build(),
				// groupBy (g) filter (g)
				a.toBuilder().groupBy(GroupByColumns.named("g")).filter(ColumnFilter.matchEq("g", "someG")).build()))
				// true because if inducer has laxer filter, there is enough groupBy to infer it
				.isTrue();

		Assertions.assertThat(splitter.canInduce(
				// groupBy (g,h) matchAll
				a.toBuilder().groupBy(GroupByColumns.named("g", "h")).filter(ISliceFilter.MATCH_ALL).build(),
				// groupBy (g) filter (g)
				a.toBuilder().groupBy(GroupByColumns.named("c")).filter(ColumnFilter.matchEq("c", "someC")).build()))
				// false because inducer lax information about c
				.isFalse();
	}

	@Test
	public void testCanInduce_sameFilterNotGroupedBy() {
		Assertions.assertThat(splitter.canInduce(
				// groupBy (g,h) filterX
				a.toBuilder()
						.groupBy(GroupByColumns.named("g", "h"))
						.filter(ColumnFilter.matchEq("c", "someC"))
						.build(),
				// groupBy (g) filterX
				a.toBuilder().groupBy(GroupByColumns.named("g")).filter(ColumnFilter.matchEq("c", "someC")).build()))
				// true because filter is identical
				.isTrue();
	}

	@Test
	public void testCanInduce_coveringFilterNotFullyGroupedBy() {
		Assertions
				.assertThat(
						splitter.canInduce(
								a.toBuilder()
										.groupBy(GroupByColumns.named("g", "h"))
										.filter(FilterBuilder
												.or(ColumnFilter.matchEq("c", "someC"),
														ColumnFilter.matchEq("g", "someG"))
												.optimize())
										.build(),
								// induced has a filter on a not groupedBy column
								a.toBuilder()
										.groupBy(GroupByColumns.named("g"))
										.filter(ColumnFilter.matchEq("c", "someC"))
										.build()))
				// while we're guaranteed to see all input, which are not able to filter out irrelevant input
				// given the filter on the not groupedBy column
				.isFalse();
	}

	@Test
	public void testCanInduce_inducedIsStricterOnGroupBy() {
		Assertions
				.assertThat(
						splitter.canInduce(
								a.toBuilder()
										.groupBy(GroupByColumns.named("a"))
										.filter(FilterBuilder
												.and(ColumnFilter.matchIn("a", "a1", "a2"),
														ColumnFilter.matchEq("b", "b3"))
												.combine())
										.build(),
								// induced has a stricter filter, based on a column which is groupedBy in the inducer
								a.toBuilder()
										.groupBy(GroupByColumns.grandTotal())
										.filter(FilterBuilder
												.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b3"))
												.combine())
										.build()))
				// true because filter is inducable (given G is groupedBy, we can ensure to get ride of C rows)
				.isTrue();
	}

	@Test
	public void testCanInduce_inducedIsStricterOnGroupBy_OrDifferentColumns_groupByLaxerColumn() {
		Assertions
				.assertThat(
						splitter.canInduce(
								a.toBuilder()
										.groupBy(GroupByColumns.named("b"))
										.filter(FilterBuilder
												.and(OrFilter.or(Map.of("a", "a1", "b", "b2")),
														ColumnFilter.matchEq("c", "c3"))
												.combine())
										.build(),
								// induced has a stricter filter, based on a column which is groupedBy in the inducer
								a.toBuilder()
										.groupBy(GroupByColumns.grandTotal())
										.filter(FilterBuilder
												.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("c", "c3"))
												.combine())
										.build()))
				// false as the removed column from the filter is not enough to check the induced filter
				.isFalse();
	}

	@Test
	public void testCanInduce_commonFilterIsNotGroupedBy() {
		Assertions
				.assertThat(splitter.canInduce(
						a.toBuilder()
								.groupBy(GroupByColumns.named("g", "h"))
								.filter(ColumnFilter.matchEq("c", "someC"))
								.build(),
						// induced has a stricter filter, based on a column which is groupedBy in the inducer
						a.toBuilder()
								.groupBy(GroupByColumns.grandTotal())
								.filter(AndFilter.and(ColumnFilter.matchEq("c", "someC"),
										ColumnFilter.matchEq("g", "someG")))
								.build()))
				// true because filter is inducable (given G is groupedBy, we can ensure to get ride of C rows)
				.isTrue();
	}

	@Test
	public void testSplitAsDag_2groupByWithNonEmptyIntersection() {
		IAdhocDag<TableQueryStep> split = splitter.splitInducedAsDag(IHasQueryOptions.noOption(),
				ImmutableSet.<TableQueryStep>builder()
						.add(a.toBuilder().groupBy(GroupByColumns.named("a", "b")).build())
						.add(a.toBuilder().groupBy(GroupByColumns.named("b", "c")).build())
						.build());

		Assertions.assertThat(split.edgeSet()).isEmpty();
	}

	@Test
	public void testSplitAsDag_missingIntermediateCardinality() {
		// groupBy sizes 1 and 3 with no size-2 step: iterating inducerGroupBy from 1 to 3
		// previously caused NPE at cardinality=2 since cardinalityToSteps.get(2) returned null
		TableQueryStep size1 = a.toBuilder().groupBy(GroupByColumns.named("a")).build();
		TableQueryStep size3 = a.toBuilder().groupBy(GroupByColumns.named("a", "b", "c")).build();

		IAdhocDag<TableQueryStep> split =
				splitter.splitInducedAsDag(IHasQueryOptions.noOption(), ImmutableSet.of(size1, size3));

		// size3 can induce size1 (it contains column "a")
		Assertions.assertThat(split.edgeSet()).hasSize(1);
		Assertions.assertThat(split.getEdgeSource(split.edgeSet().iterator().next())).isEqualTo(size1);
		Assertions.assertThat(split.getEdgeTarget(split.edgeSet().iterator().next())).isEqualTo(size3);
	}

	@Test
	public void testSplitAsDag_2groupByWithNonEmptyIntersection_andIntersection() {
		@NonNull
		TableQueryStep s1 = a.toBuilder().groupBy(GroupByColumns.named("a", "b")).build();
		@NonNull
		TableQueryStep s2 = a.toBuilder().groupBy(GroupByColumns.named("b", "c")).build();
		@NonNull
		TableQueryStep s3 = a.toBuilder().groupBy(GroupByColumns.named("b")).build();
		IAdhocDag<TableQueryStep> split = splitter.splitInducedAsDag(IHasQueryOptions.noOption(),
				ImmutableSet.<TableQueryStep>builder().add(s1).add(s2).add(s3).build());

		Assertions.assertThat(split.vertexSet()).hasSize(3);
		Assertions.assertThat(split.edgeSet())
				.hasSize(2)
				.anySatisfy(assertInduce(s3, s1, split))
				.anySatisfy(assertInduce(s3, s2, split));
	}

	@Test
	public void chainOfInducers_filters_inSameColumn() {
		TableQueryStep s1 =
				a.toBuilder().filter(ColumnFilter.matchIn("a", "a1")).groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s2 =
				a.toBuilder().filter(ColumnFilter.matchIn("a", "a1", "a2")).groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s3 = a.toBuilder()
				.filter(ColumnFilter.matchIn("a", "a1", "a2", "a3"))
				.groupBy(GroupByColumns.named("a"))
				.build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(s1);
		merged.addVertex(s2);
		merged.addVertex(s3);

		merged.addEdge(s1, s3);
		merged.addEdge(s2, s3);

		IAdhocDag<TableQueryStep> withShared =
				splitter.splitInducedAsDag(IHasQueryOptions.noOption(), ImmutableSet.of(s1, s2, s3));

		Assertions.assertThat(withShared.vertexSet()).hasSize(3).contains(s1, s2, s3);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(2)
				.anySatisfy(assertInduce(s1, s2, withShared))
				.anySatisfy(assertInduce(s2, s3, withShared));
	}

	private ThrowingConsumer<? super DefaultEdge> assertInduce(TableQueryStep s1,
			TableQueryStep s2,
			IAdhocDag<TableQueryStep> withShared) {
		return edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s1);
			Assertions.assertThat(inducer).isEqualTo(s2);
		};
	}

	@Test
	public void chainOfInducers_filters_differentColumns() {
		TableQueryStep s1 = a.toBuilder().filter(AndFilter.and(Map.of("a", "a1", "b", "b1"))).build();
		TableQueryStep s2 =
				a.toBuilder().filter(AndFilter.and(Map.of("a", "a1"))).groupBy(GroupByColumns.named("a", "b")).build();
		TableQueryStep s3 =
				a.toBuilder().filter(AndFilter.and(Map.of())).groupBy(GroupByColumns.named("a", "b", "c")).build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(s1);
		merged.addVertex(s2);
		merged.addVertex(s3);

		merged.addEdge(s1, s3);
		merged.addEdge(s2, s3);

		IAdhocDag<TableQueryStep> withShared =
				splitter.splitInducedAsDag(IHasQueryOptions.noOption(), Set.of(s1, s2, s3));

		Assertions.assertThat(withShared.vertexSet()).hasSize(3).contains(s1, s2, s3);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(2)
				.anySatisfy(assertInduce(s1, s2, withShared))
				.anySatisfy(assertInduce(s2, s3, withShared));
	}

	@Test
	public void matchNone() {
		TableQueryStep s1 = a.toBuilder().filter(AndFilter.and(Map.of("a", "a1", "b", "b1"))).build();
		TableQueryStep s2 = a.toBuilder().filter(ISliceFilter.MATCH_NONE).build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(s1);
		merged.addVertex(s2);

		IAdhocDag<TableQueryStep> withShared = splitter.splitInducedAsDag(IHasQueryOptions.noOption(), Set.of(s1, s2));

		Assertions.assertThat(withShared.vertexSet()).hasSize(2).contains(s1, s2);

		Assertions.assertThat(withShared.edgeSet()).hasSize(0);
	}
}
