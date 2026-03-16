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

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.OrFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestInduceByAdhoc {
	CubeQueryStep step = CubeQueryStep.builder()
			.measure("m1")
			.groupBy(GroupByColumns.named("g", "h"))
			.filter(ColumnFilter.matchEq("c", "c1"))
			.build();

	InduceByAdhoc splitter = new InduceByAdhoc();

	@Test
	public void testCanInduce_Trivial() {
		// Different measure by reference
		Assertions.assertThat(splitter.canInduce(step, CubeQueryStep.edit(step).measure("m2").build())).isFalse();
		// Differnt measure with same name
		Assertions.assertThat(splitter.canInduce(step, CubeQueryStep.edit(step).measure(Aggregator.sum("m1")).build()))
				.isTrue();

		// Less columns
		Assertions
				.assertThat(
						splitter.canInduce(step, CubeQueryStep.edit(step).groupBy(GroupByColumns.named("g")).build()))
				.isTrue();
		// More columns
		Assertions
				.assertThat(splitter.canInduce(step,
						CubeQueryStep.edit(step).groupBy(GroupByColumns.named("g", "h", "i")).build()))
				.isFalse();

		// Different column same coordinate
		Assertions
				.assertThat(splitter.canInduce(step,
						CubeQueryStep.edit(step).filter(ColumnFilter.matchEq("d", "c1")).build()))
				.isFalse();
	}

	@Test
	public void testCanInduce_OrDifferentColumns() {
		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step)
						.filter(FilterBuilder.or(ColumnFilter.matchEq("c", "c1"), ColumnFilter.matchEq("d", "d1"))
								.optimize())
						.build(),
				// induced has only one of filters
				CubeQueryStep.edit(step).filter(ColumnFilter.matchEq("c", "c1")).build()))
				// false because filtered columns are not groupedBy
				.isFalse();

		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g", "h"))
						.filter(FilterBuilder.or(ColumnFilter.matchEq("g", "g1"), ColumnFilter.matchEq("h", "h1"))
								.optimize())
						.build(),
				// induced has only one of filters
				CubeQueryStep.edit(step).filter(ColumnFilter.matchEq("g", "g1")).build()))
				// true because filtered columns are groupedBy: irrelevant `g` can be filtered.
				.isTrue();

		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g", "h"))
						.filter(FilterBuilder.or(ColumnFilter.matchEq("g", "g1"), ColumnFilter.matchEq("c", "c1"))
								.optimize())
						.build(),
				// induced has only one of filters
				CubeQueryStep.edit(step).filter(ColumnFilter.matchEq("g", "g1")).build()))
				// true because inducer has more rows than induced, and these rows can be filtered out (based on `g`)
				.isTrue();
	}

	@Test
	public void testCanInduce_AndDifferentColumns() {
		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step).filter(ColumnFilter.matchEq("c", "c1")).build(),
				// induced has only one of filters
				CubeQueryStep.edit(step)
						.filter(AndFilter.and(ColumnFilter.matchEq("c", "c1"), ColumnFilter.matchEq("d", "c1")))
						.build()))
				// false because filtered columns are not groupedBy
				.isFalse();

		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step).filter(ColumnFilter.matchEq("g", "c1")).build(),
				// induced has only one of filters
				CubeQueryStep.edit(step)
						.filter(AndFilter.and(ColumnFilter.matchEq("g", "c1"), ColumnFilter.matchEq("h", "c1")))
						.build()))
				// true because filtered columns are groupedBy
				.isTrue();

		Assertions.assertThat(splitter.canInduce(
				// inducer has OR on different columns
				CubeQueryStep.edit(step).filter(ColumnFilter.matchEq("g", "c1")).build(),
				// induced has only one of filters
				CubeQueryStep.edit(step)
						.filter(AndFilter.and(ColumnFilter.matchEq("g", "c1"), ColumnFilter.matchEq("c", "c1")))
						.build()))
				// false because inducer lacks information to filter along c
				.isFalse();
	}

	@Test
	public void testCanInduce_Same() {
		Assertions.assertThat(splitter.canInduce(
				CubeQueryStep.edit(step)
						.filter(ColumnFilter.matchEq("c", "c1"))
						.groupBy(GroupByColumns.named("d"))
						.build(),
				CubeQueryStep.edit(step)
						.filter(ColumnFilter.matchEq("c", "c1"))
						.groupBy(GroupByColumns.named("d"))
						.build()))
				.isTrue();
	}

	@Test
	public void testCanInduce_DifferentTopology() {
		Assertions.assertThat(splitter.canInduce(
				// groupBy (g,h) matchAll
				CubeQueryStep.edit(step).groupBy(GroupByColumns.named("g", "h")).filter(ISliceFilter.MATCH_ALL).build(),
				// groupBy (g) filter (g)
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g"))
						.filter(ColumnFilter.matchEq("g", "someG"))
						.build()))
				// true because if inducer has laxer filter, there is enough groupBy to infer it
				.isTrue();

		Assertions.assertThat(splitter.canInduce(
				// groupBy (g,h) matchAll
				CubeQueryStep.edit(step).groupBy(GroupByColumns.named("g", "h")).filter(ISliceFilter.MATCH_ALL).build(),
				// groupBy (g) filter (g)
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("c"))
						.filter(ColumnFilter.matchEq("c", "someC"))
						.build()))
				// false because inducer lax information about c
				.isFalse();
	}

	@Test
	public void testCanInduce_sameFilterNotGroupedBy() {
		Assertions.assertThat(splitter.canInduce(
				// groupBy (g,h) filterX
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g", "h"))
						.filter(ColumnFilter.matchEq("c", "someC"))
						.build(),
				// groupBy (g) filterX
				CubeQueryStep.edit(step)
						.groupBy(GroupByColumns.named("g"))
						.filter(ColumnFilter.matchEq("c", "someC"))
						.build()))
				// true because filter is identical
				.isTrue();
	}

	@Test
	public void testCanInduce_coveringFilterNotFullyGroupedBy() {
		Assertions
				.assertThat(
						splitter.canInduce(
								CubeQueryStep.edit(step)
										.groupBy(GroupByColumns.named("g", "h"))
										.filter(FilterBuilder
												.or(ColumnFilter.matchEq("c", "someC"),
														ColumnFilter.matchEq("g", "someG"))
												.optimize())
										.build(),
								// induced has a filter on a not groupedBy column
								CubeQueryStep.edit(step)
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
								CubeQueryStep.edit(step)
										.groupBy(GroupByColumns.named("a"))
										.filter(FilterBuilder
												.and(ColumnFilter.matchIn("a", "a1", "a2"),
														ColumnFilter.matchEq("b", "b3"))
												.combine())
										.build(),
								// induced has a stricter filter, based on a column which is groupedBy in the inducer
								CubeQueryStep.edit(step)
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
								CubeQueryStep.edit(step)
										.groupBy(GroupByColumns.named("b"))
										.filter(FilterBuilder
												.and(OrFilter.or(Map.of("a", "a1", "b", "b2")),
														ColumnFilter.matchEq("c", "c3"))
												.combine())
										.build(),
								// induced has a stricter filter, based on a column which is groupedBy in the inducer
								CubeQueryStep.edit(step)
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
						CubeQueryStep.edit(step)
								.groupBy(GroupByColumns.named("g", "h"))
								.filter(ColumnFilter.matchEq("c", "someC"))
								.build(),
						// induced has a stricter filter, based on a column which is groupedBy in the inducer
						CubeQueryStep.edit(step)
								.groupBy(GroupByColumns.grandTotal())
								.filter(AndFilter.and(ColumnFilter.matchEq("c", "someC"),
										ColumnFilter.matchEq("g", "someG")))
								.build()))
				// true because filter is inducable (given G is groupedBy, we can ensure to get ride of C rows)
				.isTrue();
	}

	@Test
	public void testSplitAsDag_2groupByWithNonEmptyIntersection() {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> split = splitter.splitInducedAsDag(IHasQueryOptions.noOption(),
				ImmutableSet.<CubeQueryStep>builder()
						.add(CubeQueryStep.builder()
								.measure(Aggregator.sum("m"))
								.groupBy(GroupByColumns.named("a", "b"))
								.build())
						.add(CubeQueryStep.builder()
								.measure(Aggregator.sum("m"))
								.groupBy(GroupByColumns.named("b", "c"))
								.build())
						.build());

		Assertions.assertThat(split.edgeSet()).isEmpty();
	}

	@Test
	public void testSplitAsDag_2groupByWithNonEmptyIntersection_andIntersection() {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> split = splitter.splitInducedAsDag(IHasQueryOptions.noOption(),
				ImmutableSet.<CubeQueryStep>builder()
						.add(CubeQueryStep.builder()
								.measure(Aggregator.sum("m"))
								.groupBy(GroupByColumns.named("a", "b"))
								.build())
						.add(CubeQueryStep.builder()
								.measure(Aggregator.sum("m"))
								.groupBy(GroupByColumns.named("b", "c"))
								.build())
						.add(CubeQueryStep.builder()
								.measure(Aggregator.sum("m"))
								.groupBy(GroupByColumns.named("b"))
								.build())
						.build());

		// TODO We should keep the 2 edges, so we decide later which one is optimal (e.g. based on induced actual sizes)
		Assertions.assertThat(split.edgeSet()).hasSize(1);
	}
}
