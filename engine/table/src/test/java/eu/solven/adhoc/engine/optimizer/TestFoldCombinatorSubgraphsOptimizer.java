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
import eu.solven.adhoc.engine.dag.fuser.CombinatorSubgraphsFuser;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.ComposedCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;

public class TestFoldCombinatorSubgraphsOptimizer {

	private CubeQueryStep step(Combinator measure) {
		return CubeQueryStep.builder().measure(measure).build();
	}

	private Combinator combinator(String name, String underlying) {
		return Combinator.builder().name(name).underlying(underlying).combinationKey(SumAggregation.KEY).build();
	}

	private Combinator twoUnderlyingCombinator(String name, String a, String b) {
		return Combinator.builder().name(name).underlying(a).underlying(b).combinationKey(SumCombination.KEY).build();
	}

	private static void addEdges(DirectedMultigraph<CubeQueryStep, DefaultEdge> mg,
			IAdhocDag<CubeQueryStep> dag,
			CubeQueryStep from,
			CubeQueryStep... tos) {
		for (CubeQueryStep to : tos) {
			mg.addEdge(from, to);
			dag.addEdge(from, to);
		}
	}

	@Test
	public void testEmptyDag_noOp() {
		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();

		new CombinatorSubgraphsFuser().fuse(mg, dag, Set.of(), Map.of());

		Assertions.assertThat(mg.vertexSet()).isEmpty();
	}

	@Test
	public void testChainOfTwo_doesNotFold_belowThreshold() {
		// root → c0 → leaf. Only c0 is foldable; chain length 1 → below minChainLength=2.
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(combinator("c0", "v"));
		CubeQueryStep step1 = step(combinator("c1", "c0"));

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step1, step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, step1, step0);
		addEdges(mg, dag, step0, stepAgg);

		new CombinatorSubgraphsFuser().fuse(mg, dag, Set.of(step1), Map.of());

		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(step1, step0, stepAgg);
	}

	@Test
	public void testChainOfThree_foldsTwoMids() {
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(combinator("c0", "v"));
		CubeQueryStep step1 = step(combinator("c1", "c0"));
		CubeQueryStep step2 = step(combinator("c2", "c1"));

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step2, step1, step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, step2, step1);
		addEdges(mg, dag, step1, step0);
		addEdges(mg, dag, step0, stepAgg);

		new CombinatorSubgraphsFuser().fuse(mg, dag, Set.of(step2), Map.of());

		Assertions.assertThat(mg.vertexSet()).hasSize(3).contains(step2, stepAgg);
		CubeQueryStep fused = mg.vertexSet().stream().filter(s -> s != step2 && s != stepAgg).findFirst().orElseThrow();
		Assertions.assertThat(((Combinator) fused.getMeasure()).getCombinationKey()).isEqualTo(ComposedCombination.KEY);
		Assertions.assertThat(mg.containsEdge(step2, fused)).isTrue();
		Assertions.assertThat(mg.containsEdge(fused, stepAgg)).isTrue();
	}

	@Test
	public void testTree_twoBranchesOneRoot_folds() {
		// root(c_root)
		// ├── c_a → A
		// └── c_b → B
		// Three foldable internals (c_root, c_a, c_b), two distinct boundary leaves (A, B).
		Aggregator aggA = Aggregator.builder().name("A").aggregationKey(SumAggregation.KEY).build();
		Aggregator aggB = Aggregator.builder().name("B").aggregationKey(SumAggregation.KEY).build();
		CubeQueryStep stepA = CubeQueryStep.builder().measure(aggA).build();
		CubeQueryStep stepB = CubeQueryStep.builder().measure(aggB).build();
		CubeQueryStep stepCa = step(combinator("c_a", "A"));
		CubeQueryStep stepCb = step(combinator("c_b", "B"));
		CubeQueryStep stepRoot = step(twoUnderlyingCombinator("c_root", "c_a", "c_b"));
		// The "real" root that consumes c_root externally — pretend it's a user-facing measure step.
		CubeQueryStep userRoot = step(combinator("user", "c_root"));

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { userRoot, stepRoot, stepCa, stepCb, stepA, stepB }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, userRoot, stepRoot);
		addEdges(mg, dag, stepRoot, stepCa);
		addEdges(mg, dag, stepRoot, stepCb);
		addEdges(mg, dag, stepCa, stepA);
		addEdges(mg, dag, stepCb, stepB);

		new CombinatorSubgraphsFuser().fuse(mg, dag, Set.of(userRoot), Map.of());

		// The three foldable internals (stepRoot, stepCa, stepCb) fold into one fused step; userRoot, stepA, stepB
		// survive.
		Assertions.assertThat(mg.vertexSet()).hasSize(4).contains(userRoot, stepA, stepB);
		CubeQueryStep fused = mg.vertexSet()
				.stream()
				.filter(s -> s != userRoot && s != stepA && s != stepB)
				.findFirst()
				.orElseThrow();
		Assertions.assertThat(((Combinator) fused.getMeasure()).getCombinationKey()).isEqualTo(ComposedCombination.KEY);
		// The fused step has TWO underlyings (the distinct boundary leaves).
		Assertions.assertThat(((Combinator) fused.getMeasure()).getUnderlyings()).containsExactlyInAnyOrder("A", "B");
		// Edges: userRoot → fused, fused → stepA, fused → stepB.
		Assertions.assertThat(mg.containsEdge(userRoot, fused)).isTrue();
		Assertions.assertThat(mg.containsEdge(fused, stepA)).isTrue();
		Assertions.assertThat(mg.containsEdge(fused, stepB)).isTrue();
	}

	@Test
	public void testTree_sharedBoundaryLeaf_appearsOnce() {
		// root
		// ├── c_a → A
		// └── c_b → A (same boundary leaf)
		// Expected fused step: 1 underlying (A), referenced twice from inside the plan.
		Aggregator aggA = Aggregator.builder().name("A").aggregationKey(SumAggregation.KEY).build();
		CubeQueryStep stepA = CubeQueryStep.builder().measure(aggA).build();
		CubeQueryStep stepCa = step(combinator("c_a", "A"));
		CubeQueryStep stepCb = step(combinator("c_b", "A"));
		CubeQueryStep stepRoot = step(twoUnderlyingCombinator("c_root", "c_a", "c_b"));
		CubeQueryStep userRoot = step(combinator("user", "c_root"));

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { userRoot, stepRoot, stepCa, stepCb, stepA }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, userRoot, stepRoot);
		addEdges(mg, dag, stepRoot, stepCa);
		addEdges(mg, dag, stepRoot, stepCb);
		addEdges(mg, dag, stepCa, stepA);
		addEdges(mg, dag, stepCb, stepA);

		new CombinatorSubgraphsFuser().fuse(mg, dag, Set.of(userRoot), Map.of());

		// userRoot + stepA + fused = 3 vertices.
		Assertions.assertThat(mg.vertexSet()).hasSize(3).contains(userRoot, stepA);
		CubeQueryStep fused =
				mg.vertexSet().stream().filter(s -> s != userRoot && s != stepA).findFirst().orElseThrow();
		// The fused step has ONE underlying (the shared A) — not two duplicates.
		Assertions.assertThat(((Combinator) fused.getMeasure()).getUnderlyings()).containsExactly("A");
		Assertions.assertThat(mg.containsEdge(userRoot, fused)).isTrue();
		Assertions.assertThat(mg.containsEdge(fused, stepA)).isTrue();
	}

	@Test
	public void testRootIsPreserved_evenIfItLooksFoldable() {
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(combinator("c0", "v"));

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, step0, stepAgg);

		new CombinatorSubgraphsFuser().fuse(mg, dag, Set.of(step0), Map.of());

		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(step0, stepAgg);
	}

	@Test
	public void testCachedStepIsPreserved() {
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(combinator("c0", "v"));
		CubeQueryStep step1 = step(combinator("c1", "c0"));
		CubeQueryStep step2 = step(combinator("c2", "c1"));

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step2, step1, step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, step2, step1);
		addEdges(mg, dag, step1, step0);
		addEdges(mg, dag, step0, stepAgg);

		Map<CubeQueryStep, ICuboid> cached = new LinkedHashMap<>();
		cached.put(step1, null);
		new CombinatorSubgraphsFuser().fuse(mg, dag, Set.of(step2), cached);

		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(step2, step1, step0, stepAgg);
	}

	@Test
	public void testMinChainLength_validation() {
		Assertions.assertThatThrownBy(() -> new CombinatorSubgraphsFuser(1))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("minChainLength");
	}

	@Test
	public void testOptimizerIsIdempotent() {
		Aggregator agg = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		CubeQueryStep stepAgg = CubeQueryStep.builder().measure(agg).build();
		CubeQueryStep step0 = step(combinator("c0", "v"));
		CubeQueryStep step1 = step(combinator("c1", "c0"));
		CubeQueryStep step2 = step(combinator("c2", "c1"));

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { step2, step1, step0, stepAgg }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		addEdges(mg, dag, step2, step1);
		addEdges(mg, dag, step1, step0);
		addEdges(mg, dag, step0, stepAgg);

		CombinatorSubgraphsFuser optimizer = new CombinatorSubgraphsFuser();
		optimizer.fuse(mg, dag, Set.of(step2), Map.of());
		Set<CubeQueryStep> afterFirst = new LinkedHashSet<>(mg.vertexSet());

		optimizer.fuse(mg, dag, Set.of(step2), Map.of());

		Assertions.assertThat(mg.vertexSet()).isEqualTo(afterFirst);
	}
}
