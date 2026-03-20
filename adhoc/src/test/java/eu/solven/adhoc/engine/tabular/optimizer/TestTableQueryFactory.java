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

import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.column.ColumnWithCalculatedCoordinates;
import eu.solven.adhoc.column.coordinate.CalculatedCoordinate;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.splitter.InduceByAdhoc;
import eu.solven.adhoc.engine.tabular.splitter.TableStepsGrouper;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.optimizer.FilterOptimizer;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV4;

public class TestTableQueryFactory {
	CubeQueryStep step = CubeQueryStep.builder()
			.measure("m1")
			.groupBy(GroupByColumns.named("g", "h"))
			.filter(ColumnFilter.matchEq("c", "c1"))
			.build();

	InduceByAdhoc splitter = new InduceByAdhoc();
	TableStepsGrouper grouper = new TableStepsGrouper();
	TableQueryFactory optimizer = new TableQueryFactory(AdhocFactories.builder().build(),
			FilterOptimizer.builder().build(),
			splitter,
			grouper);

	@Test
	public void testSplit_disjoint_noMeasure() {
		TableQuery tq1 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("a", "a1"))
				.groupBy(GroupByColumns.named("b"))
				.aggregator(Aggregator.sum("m1"))
				.build();
		TableQuery tq2 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("c", "c1"))
				.groupBy(GroupByColumns.named("d"))
				.aggregator(Aggregator.sum("m1"))
				.build();
		SplitTableQueries split = optimizer.splitInducedLegacy(() -> Set.of(), Set.of(tq1, tq2));

		Assertions.assertThat(split.getInducers())
				.hasSize(2)
				.contains(CubeQueryStep.edit(tq1).measure(Aggregator.sum("m1")).build())
				.contains(CubeQueryStep.edit(tq2).measure(Aggregator.sum("m1")).build());
	}

	@Test
	public void testProcessRelatedSteps_grandTotalAndGroupBy() {
		TableQueryV4 output = optimizer.processRelatedSteps(CubeQueryStep.builder().measure("m").build(),
				List.of(CubeQueryStep.builder().measure(Aggregator.empty()).build(),
						CubeQueryStep.builder()
								.measure(Aggregator.empty())
								.groupBy(GroupByColumns.named("c"))
								.build()));

		Assertions.assertThat(output.getGroupByToAggregators().keySet()).hasSize(2);
		Assertions.assertThat(output.streamV3().toList()).hasSize(1);
	}

	@Test
	public void testProcessRelatedSteps_oneCalculated() {
		TableQueryV4 output = optimizer.processRelatedSteps(CubeQueryStep.builder().measure("m").build(),
				List.of(CubeQueryStep.builder()
						.measure(Aggregator.empty())
						.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
								.column("c")
								.calculatedCoordinate(CalculatedCoordinate.star())
								.build()))
						.build()));

		Assertions.assertThat(output.getGroupByToAggregators().keySet()).hasSize(1);
		Assertions.assertThat(output.streamV3().toList()).hasSize(1);
	}

	@Test
	public void testProcessRelatedSteps_twoCalculated() {
		TableQueryV4 output = optimizer.processRelatedSteps(CubeQueryStep.builder().measure("m").build(),
				List.of(CubeQueryStep.builder()
						.measure(Aggregator.empty())
						.groupBy(GroupByColumns.of(
								ColumnWithCalculatedCoordinates.builder()
										.column("c")
										.calculatedCoordinate(CalculatedCoordinate.star())
										.build(),
								ColumnWithCalculatedCoordinates.builder()
										.column("d")
										.calculatedCoordinate(CalculatedCoordinate.star())
										.build()))
						.build()));

		Assertions.assertThat(output.getGroupByToAggregators().keySet()).hasSize(1);
		Assertions.assertThat(output.streamV3().toList()).hasSize(1);
	}

	@Test
	public void testSanityChecks() {
		TableQuery tq1 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("a", "a1"))
				.groupBy(GroupByColumns.named("b"))
				.aggregator(Aggregator.sum("m1"))
				.build();
		TableQuery tq2 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("c", "c1"))
				.groupBy(GroupByColumns.named("d"))
				.aggregator(Aggregator.sum("m1"))
				.build();

		SplitTableQueries split = optimizer.splitInducedLegacy(() -> Set.of(), Set.of(tq1, tq2));
		// Check default is safe
		optimizer.sanityChecks(split);

		// Simulate an orphan inducerStep which is not covered by tableQueries
		{
			// https://stackoverflow.com/questions/14938591/how-to-copy-a-graph-in-jgrapht
			DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> copytableStepsDag =
					(DirectedAcyclicGraph<CubeQueryStep, DefaultEdge>) split.getInducedToInducer().clone();

			copytableStepsDag.addVertex(CubeQueryStep.builder().measure("m").build());

			SplitTableQueries splitWithAdditionalExplicit =
					split.toBuilder().inducedToInducer(copytableStepsDag).build();
			Assertions.assertThatThrownBy(() -> optimizer.sanityChecks(splitWithAdditionalExplicit))
					.isInstanceOf(IllegalStateException.class)
					.hasMessage("Missing 1 steps from tableQueries to cover inducers");
		}

		// Simulate an orphan tableStep from cube DAG
		{
			SplitTableQueries splitWithAdditionalExplicit =
					split.toBuilder().explicit(CubeQueryStep.builder().measure("m").build()).build();
			Assertions.assertThatThrownBy(() -> optimizer.sanityChecks(splitWithAdditionalExplicit))
					.isInstanceOf(IllegalStateException.class)
					.hasMessage("Missing 1 steps from tableQueries+induceProcess to fill cube DAG tableSteps");
		}
	}

}
