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
package eu.solven.adhoc.engine.observability;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.IQueryStepsDagBuilder;
import eu.solven.adhoc.engine.QueryStepsDagBuilder;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.InMemoryTable;

public class TestDagExplainer implements IAdhocTestConstants {
	EventBus eventBus = new EventBus();
	List<String> messagesPerf = AdhocExplainerTestHelper.listenForPerf(eventBus);
	List<String> messagesExplain = AdhocExplainerTestHelper.listenForExplainNoPerf(eventBus);

	@Test
	public void testPerfLog() {
		DagExplainer dagExplainer = DagExplainer.builder().eventBus(eventBus::post).build();

		Assertions
				.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(Aggregator.countAsterisk()).build()))
				.isEqualTo("COUNT");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(k1PlusK2AsExpr).build()))
				.isEqualTo("Combinator[EXPRESSION]");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(filterK1onA1).build()))
				.isEqualTo("Filtrator");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(unfilterOnA).build()))
				.isEqualTo("Unfiltrator");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(sum_MaxK1K2ByA).build()))
				.isEqualTo("Partitionor[MAX][SUM]");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(dispatchFrom0To100).build()))
				.isEqualTo("Dispatchor[linear][SUM]");

	}

	// Ensure toString() is concise not to pollute the explain
	@Test
	public void testToString() {
		DagExplainer dagExplainer = DagExplainer.builder().eventBus(eventBus::post).build();

		Assertions.assertThat(dagExplainer.toString()).isEqualTo(DagExplainer.class.getSimpleName());
	}

	@Test
	public void testExplain_singleNode() {
		DagExplainer dagExplainer = DagExplainer.builder().eventBus(eventBus::post).build();

		QueryPod queryPod = QueryPod.forTable(InMemoryTable.builder().build());
		IQueryStepsDagBuilder builder = QueryStepsDagBuilder.make(AdhocFactories.builder().build(), queryPod);

		Set<IMeasure> measures = ImmutableSet.<IMeasure>builder().add(Aggregator.sum("k")).build();

		builder.registerRootWithDescendants(measures);

		dagExplainer.explain(AdhocQueryId.from("someCube", CubeQuery.builder().measure("m").build()),
				builder.getQueryDag());

		Assertions.assertThat(messagesExplain.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				/-- #0 c=someCube id=00000000-0000-0000-0000-000000000001
				\\-- #1 m=k(SUM) filter=matchAll groupBy=grandTotal""");
	}

	@Test
	public void testExplain_withChildren() {
		DagExplainer dagExplainer = DagExplainer.builder().eventBus(eventBus::post).build();

		Aggregator k1 = Aggregator.sum("k1");
		Aggregator k2 = Aggregator.sum("k2");
		IMeasure sumK1K2 = Combinator.sum(k1.getName(), k2.getName());
		Set<IMeasure> measures = ImmutableSet.<IMeasure>builder().add(k1, k2, sumK1K2).build();

		QueryPod queryPod = QueryPod.forTable(InMemoryTable.builder().build())
				.toBuilder()
				.forest(UnsafeMeasureForest.fromMeasures(this.getClass().getSimpleName(), measures).build())
				.build();
		IQueryStepsDagBuilder builder = QueryStepsDagBuilder.make(AdhocFactories.builder().build(), queryPod);

		builder.registerRootWithDescendants(Set.of(sumK1K2));

		dagExplainer.explain(AdhocQueryId.from("someCube", CubeQuery.builder().measure("m").build()),
				builder.getQueryDag());

		Assertions.assertThat(messagesExplain.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				/-- #0 c=someCube id=00000000-0000-0000-0000-000000000001
				\\-- #1 m=sum(k1,k2)(Combinator[SUM]) filter=matchAll groupBy=grandTotal
				    |\\- #2 m=k1(SUM) filter=matchAll groupBy=grandTotal
				    \\-- #3 m=k2(SUM) filter=matchAll groupBy=grandTotal""");
	}

	@Test
	public void testExplain_withChildrenAndReferences() {
		DagExplainer dagExplainer = DagExplainer.builder().eventBus(eventBus::post).build();

		Aggregator k1 = Aggregator.sum("k1");
		Aggregator k2 = Aggregator.sum("k2");
		IMeasure sumK1K2 = Combinator.sum(k1.getName(), k2.getName());
		Set<IMeasure> measures = ImmutableSet.<IMeasure>builder().add(k1, k2, sumK1K2).build();

		QueryPod queryPod = QueryPod.forTable(InMemoryTable.builder().build())
				.toBuilder()
				.forest(UnsafeMeasureForest.fromMeasures(this.getClass().getSimpleName(), measures).build())
				.build();
		IQueryStepsDagBuilder builder = QueryStepsDagBuilder.make(AdhocFactories.builder().build(), queryPod);

		builder.registerRootWithDescendants(measures);

		dagExplainer.explain(AdhocQueryId.from("someCube", CubeQuery.builder().measure("m").build()),
				builder.getQueryDag());

		Assertions.assertThat(messagesExplain.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				/-- #0 c=someCube id=00000000-0000-0000-0000-000000000001
				|\\- #1 m=k1(SUM) filter=matchAll groupBy=grandTotal
				|\\- #2 m=k2(SUM) filter=matchAll groupBy=grandTotal
				\\-- #3 m=sum(k1,k2)(Combinator[SUM]) filter=matchAll groupBy=grandTotal
				    |\\- !1
				    \\-- !2""");
	}
}
