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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.QueryStepsDag;
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

		QueryStepsDag fusedDag = new CombinatorSubgraphsFuser()
				.fuse(QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).build());
		mg = fusedDag.getMultigraph();
		dag = fusedDag.getInducedToInducer();

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

		QueryStepsDag fusedDag = new CombinatorSubgraphsFuser()
				.fuse(QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(step1)).build());
		mg = fusedDag.getMultigraph();
		dag = fusedDag.getInducedToInducer();

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

		QueryStepsDag fusedDag = new CombinatorSubgraphsFuser()
				.fuse(QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(step2)).build());
		mg = fusedDag.getMultigraph();
		dag = fusedDag.getInducedToInducer();

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

		QueryStepsDag fusedDag = new CombinatorSubgraphsFuser()
				.fuse(QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(userRoot)).build());
		mg = fusedDag.getMultigraph();
		dag = fusedDag.getInducedToInducer();

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

		QueryStepsDag fusedDag = new CombinatorSubgraphsFuser()
				.fuse(QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(userRoot)).build());
		mg = fusedDag.getMultigraph();
		dag = fusedDag.getInducedToInducer();

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

		QueryStepsDag fusedDag = new CombinatorSubgraphsFuser()
				.fuse(QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(step0)).build());
		mg = fusedDag.getMultigraph();
		dag = fusedDag.getInducedToInducer();

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

		// QueryStepsDag.stepToValues is ImmutableMap (no null values). The fuser only inspects keys, so a Mockito
		// mock stands in for the cached cuboid.
		Map<CubeQueryStep, ICuboid> cached = new LinkedHashMap<>();
		cached.put(step1, org.mockito.Mockito.mock(ICuboid.class));
		QueryStepsDag fusedDag = new CombinatorSubgraphsFuser().fuse(QueryStepsDag.builder()
				.multigraph(mg)
				.inducedToInducer(dag)
				.explicits(Set.of(step2))
				.stepToValues(cached)
				.build());
		mg = fusedDag.getMultigraph();
		dag = fusedDag.getInducedToInducer();

		Assertions.assertThat(mg.vertexSet()).containsExactlyInAnyOrder(step2, step1, step0, stepAgg);
	}

	@Test
	public void testMinChainLength_validation() {
		Assertions.assertThatThrownBy(() -> new CombinatorSubgraphsFuser(1))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("minChainLength");
	}

	/**
	 * A consumer Combinator has two underlyings where the foldable chain is at position 0 (FIRST underlying). After
	 * folding, the consumer's outgoing edges must stay in the original positional order ({@code [fused, leaf]})
	 * because the engine matches them positionally against {@link Combinator#getUnderlyings()}. The fuser snapshots
	 * the consumer's outgoing-edge order before mutating and rebuilds it with {@code top → fusedStep} substituted,
	 * so the slot the original top occupied carries the fused replacement. Without this, every position-sensitive
	 * combination (DIVIDE, SUBTRACT, …) on a measure shaped this way would silently swap its operands.
	 */
	@Test
	public void testConsumerOutgoingEdgeOrder_preservedAfterFold() {
		// consumer = Combinator with two underlyings, the FIRST one being foldable.
		Aggregator aggA = Aggregator.builder().name("a").aggregationKey(SumAggregation.KEY).build();
		Aggregator aggV = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		CubeQueryStep stepA = CubeQueryStep.builder().measure(aggA).build();
		CubeQueryStep stepV = CubeQueryStep.builder().measure(aggV).build();
		// c0 → c1 is a 2-step foldable chain ending at the leaf v.
		CubeQueryStep step0 = step(combinator("c0", "v"));
		CubeQueryStep step1 = step(combinator("c1", "c0"));
		// consumer is a two-underlying Combinator: underlying[0]="c1" (foldable chain top), underlying[1]="a".
		CubeQueryStep consumer = step(twoUnderlyingCombinator("user", "c1", "a"));

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { consumer, step1, step0, stepA, stepV }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		// Order matters here: step1 edge added FIRST (position 0 — the foldable chain), stepA edge SECOND (position 1).
		addEdges(mg, dag, consumer, step1);
		addEdges(mg, dag, consumer, stepA);
		addEdges(mg, dag, step1, step0);
		addEdges(mg, dag, step0, stepV);

		QueryStepsDag fusedDag = new CombinatorSubgraphsFuser()
				.fuse(QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(consumer)).build());

		// Consumer survives, with its two outgoing edges. The fused step must occupy what used to be step1's slot
		// (position 0 — the foldable chain's original position); the leaf Aggregator a must stay at position 1.
		List<CubeQueryStep> consumerTargets = fusedDag.getMultigraph()
				.outgoingEdgesOf(consumer)
				.stream()
				.map(e -> fusedDag.getMultigraph().getEdgeTarget(e))
				.toList();
		Assertions.assertThat(consumerTargets).hasSize(2);
		Assertions.assertThat(consumerTargets.get(0))
				.as("position 0 must be the fused step (replacing the originally-first underlying 'c1')")
				.isNotEqualTo(stepA)
				.satisfies(target -> Assertions.assertThat(((Combinator) target.getMeasure()).getCombinationKey())
						.isEqualTo(ComposedCombination.KEY));
		Assertions.assertThat(consumerTargets.get(1))
				.as("position 1 must still be the original leaf Aggregator 'a'")
				.isEqualTo(stepA);
	}

	/**
	 * The fuser folds a subgraph whose <em>top</em> has multiple consumers. Each consumer gets rewired to the same
	 * fused step.
	 *
	 * <p>
	 * Shape: {@code A → B → C → D → leaf} and {@code A → E → B}. {@code A} has two underlyings, {@code [B, E]}.
	 * {@code E} has one underlying, {@code [B]}. So {@code B} has two incoming edges (one from {@code A}, one from
	 * {@code E}). {@code B}, {@code C}, {@code D} are foldable Combinators; {@code B} is the top of a fold whose
	 * internals are {@code {B, C, D}} and boundary is {@code {leaf}}.
	 *
	 * <p>
	 * The math: folding {@code {B, C, D}} into a single {@code fusedStep} preserves the output. {@code fusedStep}'s
	 * value equals {@code B}'s former value (the composition is mathematically equivalent to walking B's subgraph).
	 * Rewiring {@code A → fusedStep} (in B's slot) and {@code E → fusedStep} gives both consumers B's value, same
	 * as before. One cuboid materialisation, two reads — no duplication, no semantic drift.
	 */
	@Test
	public void testMultiConsumerTop_foldsThroughBothConsumers() {
		// Leaf below the foldable chain.
		Aggregator aggLeaf = Aggregator.builder().name("v").aggregationKey(SumAggregation.KEY).build();
		CubeQueryStep stepLeaf = CubeQueryStep.builder().measure(aggLeaf).build();
		// Foldable chain B → C → D.
		CubeQueryStep stepD = step(combinator("D", "v"));
		CubeQueryStep stepC = step(combinator("C", "D"));
		CubeQueryStep stepB = step(combinator("B", "C"));
		// E is a Combinator consuming B (single underlying).
		CubeQueryStep stepE = step(combinator("E", "B"));
		// A is the user-requested measure: two underlyings, [B, E] in that positional order.
		CubeQueryStep stepA = step(twoUnderlyingCombinator("A", "B", "E"));

		DirectedMultigraph<CubeQueryStep, DefaultEdge> mg = new DirectedMultigraph<>(DefaultEdge.class);
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		for (CubeQueryStep s : new CubeQueryStep[] { stepA, stepE, stepB, stepC, stepD, stepLeaf }) {
			mg.addVertex(s);
			dag.addVertex(s);
		}
		// A → B (position 0), A → E (position 1).
		addEdges(mg, dag, stepA, stepB);
		addEdges(mg, dag, stepA, stepE);
		// E → B (E's single underlying).
		addEdges(mg, dag, stepE, stepB);
		// B → C → D → leaf — the foldable chain.
		addEdges(mg, dag, stepB, stepC);
		addEdges(mg, dag, stepC, stepD);
		addEdges(mg, dag, stepD, stepLeaf);

		QueryStepsDag fusedDag = new CombinatorSubgraphsFuser().fuse(QueryStepsDag.builder()
				.multigraph(mg)
				.inducedToInducer(dag)
				.explicits(Set.of(stepA))
				.build());

		// All three chain nodes (B, C, D) fold into ONE fused step.
		Assertions.assertThat(fusedDag.getMultigraph().vertexSet())
				.contains(stepA, stepE, stepLeaf)
				.doesNotContain(stepB, stepC, stepD);

		// A's outgoing edges in positional order: [fusedStep (slot 0, replacing B), stepE (slot 1, unchanged)].
		List<CubeQueryStep> aTargets = fusedDag.getMultigraph()
				.outgoingEdgesOf(stepA)
				.stream()
				.map(e -> fusedDag.getMultigraph().getEdgeTarget(e))
				.toList();
		Assertions.assertThat(aTargets).hasSize(2);
		Assertions.assertThat(aTargets.get(0))
				.as("A's position-0 underlying (originally B) must be the fused step")
				.satisfies(t -> Assertions.assertThat(((Combinator) t.getMeasure()).getCombinationKey())
						.isEqualTo(ComposedCombination.KEY));
		Assertions.assertThat(aTargets.get(1)).as("A's position-1 underlying (E) survives untouched").isEqualTo(stepE);

		// E's outgoing edge now points at the same fused step that A's slot-0 points to.
		List<CubeQueryStep> eTargets = fusedDag.getMultigraph()
				.outgoingEdgesOf(stepE)
				.stream()
				.map(e -> fusedDag.getMultigraph().getEdgeTarget(e))
				.toList();
		Assertions.assertThat(eTargets).hasSize(1);
		Assertions.assertThat(eTargets.get(0)).as("E's underlying must be the SAME fused step A points to (no duplication)")
				.isEqualTo(aTargets.get(0));
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
		QueryStepsDag first = optimizer
				.fuse(QueryStepsDag.builder().multigraph(mg).inducedToInducer(dag).explicits(Set.of(step2)).build());
		Set<CubeQueryStep> afterFirst = new LinkedHashSet<>(first.getMultigraph().vertexSet());

		QueryStepsDag second = optimizer.fuse(first);

		Assertions.assertThat(second.getMultigraph().vertexSet()).isEqualTo(afterFirst);
	}
}
