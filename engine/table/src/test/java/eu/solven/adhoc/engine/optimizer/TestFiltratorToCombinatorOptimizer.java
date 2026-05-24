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

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.dag.AdhocDag;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.dag.fuser.FiltratorToCombinatorFuser;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Filtrator;

public class TestFiltratorToCombinatorOptimizer {

	private static void addEdges(DirectedMultigraph<CubeQueryStep, DefaultEdge> mg,
			IAdhocDag<CubeQueryStep> dag,
			CubeQueryStep from,
			CubeQueryStep to) {
		mg.addEdge(from, to);
		dag.addEdge(from, to);
	}

	@Test
	public void testMatchAllFiltrator_rewritesAsCoalesceCombinator() {
		// Filtrator with filter=matchAll is trivially a passthrough.
		Aggregator agg = Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build();
		Filtrator filtrator = Filtrator.builder().name("f").underlying("d").filter(ISliceFilter.MATCH_ALL).build();
		Combinator consumer =
				Combinator.builder().name("user").underlying("f").combinationKey(CoalesceCombination.KEY).build();

		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep stepFiltrator = CubeQueryStep.builder().measure(filtrator).build();
		CubeQueryStep stepConsumer = CubeQueryStep.builder().measure(consumer).build();

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepConsumer, stepFiltrator, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, stepConsumer, stepFiltrator);
		addEdges(mg, dag, stepFiltrator, stepAgg);

		new FiltratorToCombinatorFuser().fuse(mg, dag, Set.of(stepConsumer), Map.of());

		// The Filtrator step is replaced by a passthrough Combinator. Consumer and underlying survive; the new
		// vertex sits between them with the same edges.
		Assertions.assertThat(mg.vertexSet()).hasSize(3).contains(stepConsumer, stepAgg);
		CubeQueryStep rewritten =
				mg.vertexSet().stream().filter(s -> s != stepConsumer && s != stepAgg).findFirst().orElseThrow();
		Combinator newMeasure = (Combinator) rewritten.getMeasure();
		Assertions.assertThat(newMeasure.getName()).isEqualTo("f");
		Assertions.assertThat(newMeasure.getCombinationKey()).isEqualTo(CoalesceCombination.KEY);
		Assertions.assertThat(newMeasure.getUnderlyings()).containsExactly("d");
		Assertions.assertThat(mg.containsEdge(stepConsumer, rewritten)).isTrue();
		Assertions.assertThat(mg.containsEdge(rewritten, stepAgg)).isTrue();
	}

	@Test
	public void testFiltratorFilterImpliedByStepFilter_rewrites() {
		// stepFilter = (country=FR), filtratorFilter = (country=FR). step.filter AND filtrator.filter == step.filter.
		ISliceFilter f = ColumnFilter.builder().column("country").matching("FR").build();
		Aggregator agg = Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build();
		Filtrator filtrator = Filtrator.builder().name("fr_only").underlying("d").filter(f).build();
		Combinator consumer =
				Combinator.builder().name("user").underlying("fr_only").combinationKey(CoalesceCombination.KEY).build();

		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).filter(f).build();
		CubeQueryStep stepFiltrator = CubeQueryStep.builder().measure(filtrator).filter(f).build();
		CubeQueryStep stepConsumer = CubeQueryStep.builder().measure(consumer).filter(f).build();

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepConsumer, stepFiltrator, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, stepConsumer, stepFiltrator);
		addEdges(mg, dag, stepFiltrator, stepAgg);

		new FiltratorToCombinatorFuser().fuse(mg, dag, Set.of(stepConsumer), Map.of());

		// The Filtrator's filter is identical to the step's filter, so it's redundant. Rewritten.
		Assertions.assertThat(mg.vertexSet()).hasSize(3).contains(stepConsumer, stepAgg);
		CubeQueryStep rewritten =
				mg.vertexSet().stream().filter(s -> s != stepConsumer && s != stepAgg).findFirst().orElseThrow();
		Assertions.assertThat(rewritten.getMeasure()).isInstanceOf(Combinator.class);
	}

	@Test
	public void testFiltratorFilterNotImpliedByStepFilter_isPreserved() {
		// stepFilter = matchAll, filtratorFilter = (country=FR). The Filtrator narrows the query; not a passthrough.
		ISliceFilter narrower = ColumnFilter.builder().column("country").matching("FR").build();
		Aggregator agg = Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build();
		Filtrator filtrator = Filtrator.builder().name("fr_only").underlying("d").filter(narrower).build();
		Combinator consumer =
				Combinator.builder().name("user").underlying("fr_only").combinationKey(CoalesceCombination.KEY).build();

		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep stepFiltrator = CubeQueryStep.builder().measure(filtrator).build();
		CubeQueryStep stepConsumer = CubeQueryStep.builder().measure(consumer).build();

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepConsumer, stepFiltrator, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, stepConsumer, stepFiltrator);
		addEdges(mg, dag, stepFiltrator, stepAgg);

		new FiltratorToCombinatorFuser().fuse(mg, dag, Set.of(stepConsumer), Map.of());

		// Filtrator narrows the query, so it MUST stay — original measure preserved.
		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(stepConsumer, stepFiltrator, stepAgg);
		Assertions.assertThat(stepFiltrator.getMeasure()).isInstanceOf(Filtrator.class);
	}

	@Test
	public void testRootFiltratorIsPreserved() {
		// A user-requested Filtrator must not be rewritten — would break the post-optimization sanity check.
		Aggregator agg = Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build();
		Filtrator filtrator = Filtrator.builder().name("f").underlying("d").filter(ISliceFilter.MATCH_ALL).build();

		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep stepFiltrator = CubeQueryStep.builder().measure(filtrator).build();

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepFiltrator, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, stepFiltrator, stepAgg);

		new FiltratorToCombinatorFuser().fuse(mg, dag, Set.of(stepFiltrator), Map.of());

		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(stepFiltrator, stepAgg);
		Assertions.assertThat(stepFiltrator.getMeasure()).isInstanceOf(Filtrator.class);
	}
}
