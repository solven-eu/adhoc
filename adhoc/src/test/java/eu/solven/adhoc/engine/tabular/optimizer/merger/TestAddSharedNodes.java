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
package eu.solven.adhoc.engine.tabular.optimizer.merger;

import java.util.Map;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphHelpers;
import eu.solven.adhoc.engine.tabular.optimizer.GraphsTestHelpers;
import eu.solven.adhoc.engine.tabular.optimizer.IAdhocDag;
import eu.solven.adhoc.engine.tabular.splitter.adder.AddSharedNodes;
import eu.solven.adhoc.engine.tabular.splitter.adder.IAddSharedNodes;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestAddSharedNodes {
	IAddSharedNodes sharedNodes = AddSharedNodes.builder().build();

	TableQueryStep a = TableQueryStep.builder().aggregator(Aggregator.sum("k")).build();

	@Test
	public void sharedFilter_sharedWithLessGroupBy() {
		TableQueryStep s1 =
				a.toBuilder().filter(AndFilter.and(Map.of("a", "a1"))).groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s2 =
				a.toBuilder().filter(AndFilter.and(Map.of("a", "a1"))).groupBy(GroupByColumns.named("b")).build();
		TableQueryStep s3 =
				a.toBuilder().filter(AndFilter.and(Map.of())).groupBy(GroupByColumns.named("a", "b", "c")).build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(s1);
		merged.addVertex(s2);
		merged.addVertex(s3);

		merged.addEdge(s1, s3);
		merged.addEdge(s2, s3);

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		TableQueryStep shared =
				a.toBuilder().filter(AndFilter.and(Map.of("a", "a1"))).groupBy(GroupByColumns.named("a", "b")).build();
		Assertions.assertThat(withShared.vertexSet()).hasSize(4).contains(s1, s2, s3).contains(shared);

		Assertions.assertThat(withShared.edgeSet()).hasSize(3).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s1);
			Assertions.assertThat(inducer).isEqualTo(shared);
		}).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s2);
			Assertions.assertThat(inducer).isEqualTo(shared);
		}).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(shared);
			Assertions.assertThat(inducer).isEqualTo(s3);
		});
	}

	// Given `a->a,b,c`, `a,b->a,b,c`, `a,c->a,b,c`
	// We should remove `a->a,b,c` and add both `a->a,c` and `a->a,b`
	@Test
	public void chainOfInducers_groupBy() {
		TableQueryStep s1 = a.toBuilder().groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s2 = a.toBuilder().groupBy(GroupByColumns.named("a", "b")).build();
		TableQueryStep s3 = a.toBuilder().groupBy(GroupByColumns.named("a", "c")).build();

		TableQueryStep globalInducer = a.toBuilder().groupBy(GroupByColumns.named("a", "b", "c")).build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(globalInducer);
		Stream.of(s1, s2, s3).forEach(s -> {
			merged.addVertex(s);
			merged.addEdge(s, globalInducer);
		});

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		Assertions.assertThat(withShared.vertexSet()).hasSize(4).contains(s1, s2, s3).contains(globalInducer);

		Assertions.assertThat(withShared.edgeSet()).hasSize(3).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s1);
			Assertions.assertThat(inducer).isEqualTo(globalInducer);
		}).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s2);
			Assertions.assertThat(inducer).isEqualTo(globalInducer);
		}).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s3);
			Assertions.assertThat(inducer).isEqualTo(globalInducer);
		});
	}

	// Given `FILTER a1`, `FILTER a1 OR a2`, `FILTER a1 OR a2 OR a3`
	// We should infer `FILTER a1` given `FILTER a1 OR a2`, itself given `FILTER a1 OR a2 OR a3`
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

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		Assertions.assertThat(withShared.vertexSet()).hasSize(3).contains(s1, s2, s3);

		Assertions.assertThat(withShared.edgeSet()).hasSize(2).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s1);
			Assertions.assertThat(inducer).isEqualTo(s3);
		}).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s2);
			Assertions.assertThat(inducer).isEqualTo(s3);
		});
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

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		Assertions.assertThat(withShared.vertexSet()).hasSize(3).contains(s1, s2, s3);

		Assertions.assertThat(withShared.edgeSet()).hasSize(2).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s1);
			Assertions.assertThat(inducer).isEqualTo(s3);
		}).anySatisfy(edge -> {
			TableQueryStep induced = withShared.getEdgeSource(edge);
			TableQueryStep inducer = withShared.getEdgeTarget(edge);

			Assertions.assertThat(induced).isEqualTo(s2);
			Assertions.assertThat(inducer).isEqualTo(s3);
		});
	}

	@Test
	public void sharedFilter_parentHasIrrelevantFilterToChildren() {
		TableQueryStep s1 = a.toBuilder()
				.filter(AndFilter.and(Map.of("a", "a1", "b", "b1")))
				.groupBy(GroupByColumns.named("a"))
				.build();
		TableQueryStep s2 = a.toBuilder()
				.filter(AndFilter.and(Map.of("a", "a1", "b", "b1")))
				.groupBy(GroupByColumns.named("b"))
				.build();
		TableQueryStep s3 = a.toBuilder()
				.filter(AndFilter.and(Map.of("a", "a1", "b", "b2")))
				.groupBy(GroupByColumns.named("b"))
				.build();
		TableQueryStep s4 = a.toBuilder()
				.filter(FilterBuilder
						.or(ColumnFilter.matchEq("c", "c1"),
								AndFilter.and(ColumnFilter.matchEq("a", "a1"),
										ColumnFilter.matchIn("b", "b1", "b2", "b3")))
						.combine())
				.groupBy(GroupByColumns.named("a", "b", "c"))
				.build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(s1);
		merged.addVertex(s2);
		merged.addVertex(s3);
		merged.addVertex(s4);

		merged.addEdge(s1, s4);
		merged.addEdge(s2, s4);
		merged.addEdge(s3, s4);

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		// Helps computing the 3 nodes
		TableQueryStep shared = a.toBuilder()
				.filter(AndFilter.and(Map.of("a", "a1", "b", InMatcher.matchIn("b1", "b2"))))
				.groupBy(GroupByColumns.named("a", "b"))
				.build();
		// Helps computing the 2 nodes on `b1`
		TableQueryStep shared2 = a.toBuilder()
				.filter(AndFilter.and(Map.of("a", "a1", "b", "b1")))
				.groupBy(GroupByColumns.named("a", "b"))
				.build();
		Assertions.assertThat(withShared.vertexSet())
				.hasSize(merged.vertexSet().size() + 2)
				.containsAll(merged.vertexSet())
				.contains(shared)
				.contains(shared2);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(5)
				.anySatisfy(GraphsTestHelpers.assertEdge(s1, shared2, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s2, shared2, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(shared2, shared, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s3, shared, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(shared, s4, withShared));
	}
}
