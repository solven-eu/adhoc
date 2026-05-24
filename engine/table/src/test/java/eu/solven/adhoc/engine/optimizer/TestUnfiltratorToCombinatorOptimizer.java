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
package eu.solven.adhoc.engine.optimizer;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.dag.AdhocDag;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.dag.fuser.UnfiltratorToCombinatorFuser;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Unfiltrator;
import eu.solven.adhoc.model.measure.Unfiltrator.Mode;

public class TestUnfiltratorToCombinatorOptimizer {

	private static void addEdges(DirectedMultigraph<CubeQueryStep, DefaultEdge> mg,
			IAdhocDag<CubeQueryStep> dag,
			CubeQueryStep from,
			CubeQueryStep to) {
		mg.addEdge(from, to);
		dag.addEdge(from, to);
	}

	@Test
	public void testMatchAllStepFilter_rewritesAsCoalesceCombinator() {
		// stepFilter=matchAll: any editor is a no-op on matchAll, so the Unfiltrator is a passthrough.
		Aggregator agg = Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build();
		Unfiltrator unfiltrator =
				Unfiltrator.builder().name("u").underlying("d").column("country").mode(Mode.Suppress).build();
		Combinator consumer =
				Combinator.builder().name("user").underlying("u").combinationKey(CoalesceCombination.KEY).build();

		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep stepUnfiltrator = CubeQueryStep.builder().measure(unfiltrator).build();
		CubeQueryStep stepConsumer = CubeQueryStep.builder().measure(consumer).build();

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepConsumer, stepUnfiltrator, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, stepConsumer, stepUnfiltrator);
		addEdges(mg, dag, stepUnfiltrator, stepAgg);

		QueryStepsDag fused = new UnfiltratorToCombinatorFuser().fuse(
				QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(stepConsumer)).build());
		mg = fused.getMultigraph();
		dag = fused.getInducedToInducer();

		Assertions.assertThat(mg.vertexSet()).hasSize(3).contains(stepConsumer, stepAgg);
		CubeQueryStep rewritten =
				mg.vertexSet().stream().filter(s -> s != stepConsumer && s != stepAgg).findFirst().orElseThrow();
		Combinator newMeasure = (Combinator) rewritten.getMeasure();
		Assertions.assertThat(newMeasure.getName()).isEqualTo("u");
		Assertions.assertThat(newMeasure.getCombinationKey()).isEqualTo(CoalesceCombination.KEY);
		Assertions.assertThat(newMeasure.getUnderlyings()).containsExactly("d");
		Assertions.assertThat(mg.containsEdge(stepConsumer, rewritten)).isTrue();
		Assertions.assertThat(mg.containsEdge(rewritten, stepAgg)).isTrue();
	}

	@Test
	public void testSuppress_columnAbsentFromStepFilter_rewrites() {
		// stepFilter restricts city only; Suppress(country) on it is a no-op — Unfiltrator collapses to passthrough.
		ISliceFilter stepFilter = ColumnFilter.builder().column("city").matching("Paris").build();
		Aggregator agg = Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build();
		Unfiltrator unfiltrator =
				Unfiltrator.builder().name("u").underlying("d").column("country").mode(Mode.Suppress).build();
		Combinator consumer =
				Combinator.builder().name("user").underlying("u").combinationKey(CoalesceCombination.KEY).build();

		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).filter(stepFilter).build();
		CubeQueryStep stepUnfiltrator = CubeQueryStep.builder().measure(unfiltrator).filter(stepFilter).build();
		CubeQueryStep stepConsumer = CubeQueryStep.builder().measure(consumer).filter(stepFilter).build();

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepConsumer, stepUnfiltrator, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, stepConsumer, stepUnfiltrator);
		addEdges(mg, dag, stepUnfiltrator, stepAgg);

		QueryStepsDag fused = new UnfiltratorToCombinatorFuser().fuse(
				QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(stepConsumer)).build());
		mg = fused.getMultigraph();
		dag = fused.getInducedToInducer();

		Assertions.assertThat(mg.vertexSet()).hasSize(3).contains(stepConsumer, stepAgg);
		CubeQueryStep rewritten =
				mg.vertexSet().stream().filter(s -> s != stepConsumer && s != stepAgg).findFirst().orElseThrow();
		Assertions.assertThat(rewritten.getMeasure()).isInstanceOf(Combinator.class);
	}

	@Test
	public void testSuppress_columnPresentInStepFilter_isPreserved() {
		// Suppress(country) genuinely widens the filter (drops the country clause) — not a passthrough.
		ISliceFilter stepFilter = ColumnFilter.builder().column("country").matching("FR").build();
		Aggregator agg = Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build();
		Unfiltrator unfiltrator =
				Unfiltrator.builder().name("u").underlying("d").column("country").mode(Mode.Suppress).build();
		Combinator consumer =
				Combinator.builder().name("user").underlying("u").combinationKey(CoalesceCombination.KEY).build();

		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).filter(stepFilter).build();
		CubeQueryStep stepUnfiltrator = CubeQueryStep.builder().measure(unfiltrator).filter(stepFilter).build();
		CubeQueryStep stepConsumer = CubeQueryStep.builder().measure(consumer).filter(stepFilter).build();

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepConsumer, stepUnfiltrator, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, stepConsumer, stepUnfiltrator);
		addEdges(mg, dag, stepUnfiltrator, stepAgg);

		QueryStepsDag fused = new UnfiltratorToCombinatorFuser().fuse(
				QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(stepConsumer)).build());
		mg = fused.getMultigraph();
		dag = fused.getInducedToInducer();

		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(stepConsumer, stepUnfiltrator, stepAgg);
		Assertions.assertThat(stepUnfiltrator.getMeasure()).isInstanceOf(Unfiltrator.class);
	}

	@Test
	public void testRetain_listedColumnEqualsFilterScope_rewrites() {
		// stepFilter restricts country only; Retain(country) keeps that — no change to the filter.
		ISliceFilter stepFilter = ColumnFilter.builder().column("country").matching("FR").build();
		Aggregator agg = Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build();
		Unfiltrator unfiltrator =
				Unfiltrator.builder().name("u").underlying("d").column("country").mode(Mode.Retain).build();
		Combinator consumer =
				Combinator.builder().name("user").underlying("u").combinationKey(CoalesceCombination.KEY).build();

		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).filter(stepFilter).build();
		CubeQueryStep stepUnfiltrator = CubeQueryStep.builder().measure(unfiltrator).filter(stepFilter).build();
		CubeQueryStep stepConsumer = CubeQueryStep.builder().measure(consumer).filter(stepFilter).build();

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepConsumer, stepUnfiltrator, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, stepConsumer, stepUnfiltrator);
		addEdges(mg, dag, stepUnfiltrator, stepAgg);

		QueryStepsDag fused = new UnfiltratorToCombinatorFuser().fuse(
				QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(stepConsumer)).build());
		mg = fused.getMultigraph();
		dag = fused.getInducedToInducer();

		Assertions.assertThat(mg.vertexSet()).hasSize(3).contains(stepConsumer, stepAgg);
		CubeQueryStep rewritten =
				mg.vertexSet().stream().filter(s -> s != stepConsumer && s != stepAgg).findFirst().orElseThrow();
		Assertions.assertThat(rewritten.getMeasure()).isInstanceOf(Combinator.class);
	}

	@Test
	public void testRootUnfiltratorIsPreserved() {
		// A user-requested Unfiltrator must not be rewritten — would break the post-optimization sanity check.
		Aggregator agg = Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build();
		Unfiltrator unfiltrator =
				Unfiltrator.builder().name("u").underlying("d").column("country").mode(Mode.Suppress).build();

		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep stepUnfiltrator = CubeQueryStep.builder().measure(unfiltrator).build();

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepUnfiltrator, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, stepUnfiltrator, stepAgg);

		QueryStepsDag fused = new UnfiltratorToCombinatorFuser().fuse(QueryStepsDag.builder()
				.multigraph(mg)
				.inducedToInducer(dag)
				.explicits(Set.of(stepUnfiltrator))
				.build());
		mg = fused.getMultigraph();
		dag = fused.getInducedToInducer();

		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(stepUnfiltrator, stepAgg);
		Assertions.assertThat(stepUnfiltrator.getMeasure()).isInstanceOf(Unfiltrator.class);
	}
}
