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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.dag.AdhocDag;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.ComposedCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;

/**
 * Unit tests for {@link FoldLinearChainsOptimizer}. We build small in-memory DAGs by hand rather than going through
 * {@code QueryStepsDagBuilder.registerRootWithDescendants}, so the assertions on the resulting graph stay focused on
 * the optimizer's rewrite logic.
 */
public class TestFoldLinearChainsOptimizer {

	private CubeQueryStep step(Combinator measure) {
		return CubeQueryStep.builder().measure(measure).build();
	}

	private Combinator combinator(String name, String underlying) {
		return Combinator.builder().name(name).underlying(underlying).combinationKey(SumAggregation.KEY).build();
	}

	@Test
	public void testEmptyDag_noOp() {
		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();

		new FoldLinearChainsOptimizer().optimize(mg, dag, Set.of(), Map.of());

		Assertions.assertThat(mg.vertexSet()).isEmpty();
	}

	@Test
	public void testChainOfTwo_folds() {
		// root (c1) → c0 → leaf (agg)
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		Combinator c0 = combinator("c0", "v");
		Combinator c1 = combinator("c1", "c0");
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(c0);
		CubeQueryStep step1 = step(c1);

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step1, step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		mg.addEdge(step1, step0);
		dag.addEdge(step1, step0);
		mg.addEdge(step0, stepAgg);
		dag.addEdge(step0, stepAgg);

		Set<CubeQueryStep> roots = Set.of(step1);
		Map<CubeQueryStep, ICuboid> stepToValue = Map.of();

		new FoldLinearChainsOptimizer().optimize(mg, dag, roots, stepToValue);

		// c0 was the only foldable node (1 in / 1 out / Combinator / not in roots) — but a single-node chain is below
		// the default minChainLength=2 → it stays untouched.
		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(step1, step0, stepAgg);
	}

	@Test
	public void testChainOfThree_foldsTwoMids() {
		// root (c2) → c1 → c0 → leaf (agg). c1 and c0 are both single-in/single-out Combinators → chain length 2 →
		// folds.
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		Combinator c0 = combinator("c0", "v");
		Combinator c1 = combinator("c1", "c0");
		Combinator c2 = combinator("c2", "c1");
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(c0);
		CubeQueryStep step1 = step(c1);
		CubeQueryStep step2 = step(c2);

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step2, step1, step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		mg.addEdge(step2, step1);
		dag.addEdge(step2, step1);
		mg.addEdge(step1, step0);
		dag.addEdge(step1, step0);
		mg.addEdge(step0, stepAgg);
		dag.addEdge(step0, stepAgg);

		Set<CubeQueryStep> roots = Set.of(step2);

		new FoldLinearChainsOptimizer().optimize(mg, dag, roots, Map.of());

		// c0 + c1 fold into one ComposedCombination-backed step; c2 (root) and the leaf survive.
		Assertions.assertThat(mg.vertexSet()).hasSize(3).contains(step2, stepAgg);
		// The fused step has c2 as consumer and the leaf as underlying.
		CubeQueryStep fused = mg.vertexSet().stream().filter(s -> s != step2 && s != stepAgg).findFirst().orElseThrow();
		Assertions.assertThat(((Combinator) fused.getMeasure()).getCombinationKey())
				.isEqualTo(ComposedCombination.class.getName());
		Assertions.assertThat(mg.containsEdge(step2, fused)).isTrue();
		Assertions.assertThat(mg.containsEdge(fused, stepAgg)).isTrue();
	}

	@Test
	public void testRootIsPreserved_evenIfItLooksFoldable() {
		// A user-requested step (in `roots`) is never folded away, even if its degrees would otherwise qualify.
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		Combinator c0 = combinator("c0", "v");
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(c0);

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		mg.addEdge(step0, stepAgg);
		dag.addEdge(step0, stepAgg);

		new FoldLinearChainsOptimizer().optimize(mg, dag, Set.of(step0), Map.of());

		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(step0, stepAgg);
	}

	@Test
	public void testCachedStepIsPreserved() {
		// A pre-cached intermediate must not be folded away — the engine pre-loaded its cuboid by key.
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		Combinator c0 = combinator("c0", "v");
		Combinator c1 = combinator("c1", "c0");
		Combinator c2 = combinator("c2", "c1");
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(c0);
		CubeQueryStep step1 = step(c1);
		CubeQueryStep step2 = step(c2);

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step2, step1, step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		mg.addEdge(step2, step1);
		dag.addEdge(step2, step1);
		mg.addEdge(step1, step0);
		dag.addEdge(step1, step0);
		mg.addEdge(step0, stepAgg);
		dag.addEdge(step0, stepAgg);

		Map<CubeQueryStep, ICuboid> cached = new LinkedHashMap<>();
		cached.put(step1, null);
		new FoldLinearChainsOptimizer().optimize(mg, dag, Set.of(step2), cached);

		// step1 is cached → not foldable. step0 alone is below minChainLength=2 → not folded.
		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(step2, step1, step0, stepAgg);
	}

	@Test
	public void testCompositeRunsDelegates_inOrder() {
		// Build the same chain-of-three fixture, then run the composite (Noop + Fold). The composite must produce
		// the same result as Fold alone.
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		Combinator c0 = combinator("c0", "v");
		Combinator c1 = combinator("c1", "c0");
		Combinator c2 = combinator("c2", "c1");
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(c0);
		CubeQueryStep step1 = step(c1);
		CubeQueryStep step2 = step(c2);

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step2, step1, step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		mg.addEdge(step2, step1);
		dag.addEdge(step2, step1);
		mg.addEdge(step1, step0);
		dag.addEdge(step1, step0);
		mg.addEdge(step0, stepAgg);
		dag.addEdge(step0, stepAgg);

		new CompositeQueryStepsDagOptimizer(new NoopQueryStepsDagOptimizer(), new FoldLinearChainsOptimizer())
				.optimize(mg, dag, Set.of(step2), Map.of());

		Assertions.assertThat(mg.vertexSet()).hasSize(3).contains(step2, stepAgg);
	}

	@Test
	public void testMinChainLength_validation() {
		Assertions.assertThatThrownBy(() -> new FoldLinearChainsOptimizer(1))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("minChainLength");
	}

	@Test
	public void testOptimizerIsIdempotent() {
		// Per the IQueryStepsDagOptimizer contract: running the same optimizer twice on the same DAG must produce
		// the same result as running it once.
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		Combinator c0 = combinator("c0", "v");
		Combinator c1 = combinator("c1", "c0");
		Combinator c2 = combinator("c2", "c1");
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(c0);
		CubeQueryStep step1 = step(c1);
		CubeQueryStep step2 = step(c2);

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step2, step1, step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		mg.addEdge(step2, step1);
		dag.addEdge(step2, step1);
		mg.addEdge(step1, step0);
		dag.addEdge(step1, step0);
		mg.addEdge(step0, stepAgg);
		dag.addEdge(step0, stepAgg);

		FoldLinearChainsOptimizer optimizer = new FoldLinearChainsOptimizer();
		optimizer.optimize(mg, dag, Set.of(step2), Map.of());
		Set<CubeQueryStep> afterFirst = new LinkedHashSet<>(mg.vertexSet());

		optimizer.optimize(mg, dag, Set.of(step2), Map.of());

		Assertions.assertThat(mg.vertexSet()).isEqualTo(afterFirst);
	}
}
