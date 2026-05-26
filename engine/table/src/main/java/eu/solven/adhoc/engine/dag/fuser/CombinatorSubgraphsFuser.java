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
package eu.solven.adhoc.engine.dag.fuser;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.ComposedCombination;
import eu.solven.adhoc.measure.combination.ComposedCombinationPlan;
import eu.solven.adhoc.measure.combination.ComposedCombinationPlan.CombineStep;
import eu.solven.adhoc.model.measure.Combinator;
import lombok.extern.slf4j.Slf4j;

/**
 * Folds maximal foldable subgraphs of {@link Combinator}-measure steps into a single fused step backed by a
 * {@link ComposedCombination}. The chain case (every internal node has exactly one underlying) is one degenerate shape;
 * the general case is a tree (a combinator with multiple underlyings, each themselves a foldable combinator chain or
 * sub-tree) — even DAGs when a non-foldable boundary leaf is shared between two foldable internals.
 *
 * <p>
 * A node is foldable iff: its measure is a {@link Combinator}, it has at least one incoming and at least one outgoing
 * edge, it is not user-requested (not in {@code roots}) and it is not pre-loaded from cache (not in
 * {@code stepToValue}). A foldable subgraph is then formed by walking down from the topmost foldable ancestor, with
 * one extra constraint on the descendants: an internal (non-top) node must have exactly ONE incoming edge — otherwise
 * folding it away would orphan whichever consumer of it sits outside the subgraph. Multi-consumer nodes that fail
 * this test are demoted to boundary leaves of the fold. The TOP of the subgraph IS allowed to have multiple incoming
 * edges; every consumer of top gets rewired to the fused step (one cuboid materialisation, N reads — no
 * duplication). A connected subgraph of {@code n >= minChainLength} internals is replaced by a single fused step
 * whose combination evaluates the captured subgraph in one per-cell pass — skipping every intermediate cuboid
 * materialisation.
 *
 * <p>
 * The fused step's underlyings are the <em>distinct</em> boundary leaves of the subgraph (a non-foldable node reachable
 * from one or more foldable internals). When the same boundary leaf is referenced from multiple foldable internals it
 * is registered once as an underlying, and the {@link ComposedCombinationPlan} routes both references through the same
 * slot — the engine still materialises that leaf's cuboid exactly once.
 *
 * <p>
 * Trade-off rationale: this optimizer adds a graph traversal at DAG-build time, but skips one cuboid materialisation
 * per folded internal at execution time. Per {@code docs/optimization.md}, the asymmetric cost is accepted: trivial
 * queries pay a small fixed overhead, complex queries save proportionally to the subgraph size.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class CombinatorSubgraphsFuser implements IQueryStepsDagFuser {

	/** Default minimum number of folded internals — per {@code docs/optimization.md}, n=2 is the right threshold. */
	public static final int DEFAULT_MIN_CHAIN_LENGTH = 2;

	protected final int minChainLength;
	protected final IComposedNameStrategy nameStrategy;

	public CombinatorSubgraphsFuser() {
		this(DEFAULT_MIN_CHAIN_LENGTH, IComposedNameStrategy.DEFAULT);
	}

	public CombinatorSubgraphsFuser(int minChainLength) {
		this(minChainLength, IComposedNameStrategy.DEFAULT);
	}

	public CombinatorSubgraphsFuser(int minChainLength, IComposedNameStrategy nameStrategy) {
		if (minChainLength < 2) {
			throw new IllegalArgumentException("minChainLength must be >= 2, got: " + minChainLength);
		}
		this.minChainLength = minChainLength;
		this.nameStrategy = nameStrategy;
	}

	@Override
	public QueryStepsDag fuse(QueryStepsDag input) {
		// Snapshot of currently foldable nodes. Foldability is a function of the graph at this instant; we recompute
		// only at the start because each subgraph rewrite is self-contained — removing internals does not change the
		// foldability of any node outside the subgraph (internals had degree 1 incoming by collectSubgraph's check, so
		// no edge crossed in).
		Set<CubeQueryStep> foldable = input.getMultigraph()
				.vertexSet()
				.stream()
				.filter(step -> isFoldable(step, input.getMultigraph(), input.getExplicits(), input.getStepToValues()))
				.collect(ImmutableSet.toImmutableSet());
		if (foldable.isEmpty()) {
			return input;
		}

		DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph =
				DagFuserHelpers.copyMultigraph(input.getMultigraph());
		IAdhocDag<CubeQueryStep> dag = DagFuserHelpers.copyDag(input.getInducedToInducer());
		boolean changed = false;

		// Iterate in forward topological order over the original DAG (roots first). This makes the search converge
		// on the right top on the first seed: an ancestor that is the top of its fold is visited before its
		// descendants, so subsequent seeds in the same subgraph are short-circuited via `processed`. Without this
		// guarantee, processing a deep seed first could spend an iteration on a too-small subgraph before its proper
		// top is reached on a later seed (still correct, just wasteful).
		Set<CubeQueryStep> processed = new LinkedHashSet<>();
		TopologicalOrderIterator<CubeQueryStep, DefaultEdge> topo =
				new TopologicalOrderIterator<>(input.getInducedToInducer());
		while (topo.hasNext()) {
			CubeQueryStep seed = topo.next();
			if (!foldable.contains(seed) || processed.contains(seed)) {
				continue;
			}
			// Find topmost foldable ancestor of seed: walk up while the parent is in `foldable`.
			CubeQueryStep top = topmostFoldable(seed, foldable, multigraph);

			// Walk down from top, collecting foldable internals (in BFS order) and distinct boundary leaves.
			Subgraph subgraph = collectSubgraph(top, foldable, multigraph);
			processed.addAll(subgraph.internals);

			if (subgraph.internals.size() < minChainLength) {
				continue;
			}
			changed |= rewrite(top, subgraph, multigraph, dag);
		}

		if (!changed) {
			return input;
		}
		return input.toBuilder().multigraph(multigraph).inducedToInducer(dag).build();
	}

	/**
	 * A node is foldable when (a) its measure is a {@link Combinator}, (b) it has at least one incoming and at least
	 * one outgoing edge, (c) it is neither user-requested nor pre-cached. The "at least one incoming" check tolerates
	 * multi-consumer tops; the additional "exactly one incoming" constraint for non-top internals is enforced inside
	 * {@link #collectSubgraph(CubeQueryStep, Set, DirectedMultigraph)}.
	 */
	protected boolean isFoldable(CubeQueryStep step,
			DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue) {
		return step.getMeasure() instanceof Combinator && !multigraph.incomingEdgesOf(step).isEmpty()
				&& !multigraph.outgoingEdgesOf(step).isEmpty()
				&& !roots.contains(step)
				&& !stepToValue.containsKey(step);
	}

	protected CubeQueryStep topmostFoldable(CubeQueryStep seed,
			Set<CubeQueryStep> foldable,
			DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph) {
		CubeQueryStep top = seed;
		while (true) {
			Set<DefaultEdge> in = multigraph.incomingEdgesOf(top);
			if (in.size() != 1) {
				break;
			}
			CubeQueryStep parent = multigraph.getEdgeSource(in.iterator().next());
			if (!foldable.contains(parent)) {
				break;
			}
			top = parent;
		}
		return top;
	}

	/**
	 * BFS down from {@code top}, accumulating every reachable foldable descendant into {@code internals} and every
	 * non-foldable (or multi-consumer) child into {@code boundary} (deduplicated; iteration order preserved so the
	 * fused step's underlying list is deterministic).
	 *
	 * <p>
	 * A child is admitted as an internal only if it is foldable AND has exactly one incoming edge — i.e. its sole
	 * consumer is the foldable parent that just brought it into the BFS. A multi-consumer child is downgraded to a
	 * boundary leaf even when its measure is foldable, because folding it away would orphan whichever consumer sits
	 * outside the subgraph. The top itself is exempt: its consumers are tracked separately in {@link #rewrite} and
	 * rewired to the fused step.
	 */
	protected Subgraph collectSubgraph(CubeQueryStep top,
			Set<CubeQueryStep> foldable,
			DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph) {
		Set<CubeQueryStep> internals = new LinkedHashSet<>();
		Set<CubeQueryStep> boundary = new LinkedHashSet<>();

		Deque<CubeQueryStep> work = new ArrayDeque<>();
		work.add(top);
		while (!work.isEmpty()) {
			CubeQueryStep node = work.poll();
			if (!internals.add(node)) {
				continue;
			}
			for (DefaultEdge outEdge : multigraph.outgoingEdgesOf(node)) {
				CubeQueryStep child = multigraph.getEdgeTarget(outEdge);
				if (foldable.contains(child) && multigraph.incomingEdgesOf(child).size() == 1) {
					work.add(child);
				} else {
					boundary.add(child);
				}
			}
		}
		return new Subgraph(internals, boundary);
	}

	/**
	 * Build the {@link ComposedCombinationPlan}, assemble the fused step, and rewrite {@code multigraph} + {@code dag}
	 * accordingly.
	 *
	 * @return true iff a fold was applied; false if the subgraph was rejected (e.g. heterogeneous filter/groupBy).
	 */
	protected boolean rewrite(CubeQueryStep top,
			Subgraph subgraph,
			DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			IAdhocDag<CubeQueryStep> dag) {
		// Preflight: heterogeneous filter / groupBy / customMarker across the subgraph indicates a node whose
		// IMeasureQueryStep manipulates the slice shape (a Filtrator-like behaviour wrapped in a Combinator). Refuse
		// to fold in that case — the per-cell composition would produce wrong values.
		for (CubeQueryStep step : subgraph.internals) {
			if (!step.getFilter().equals(top.getFilter()) || !step.getGroupBy().equals(top.getGroupBy())
					|| !Objects.equals(step.getCustomMarker(), top.getCustomMarker())) {
				log.debug("Heterogeneous filter/groupBy/customMarker in subgraph anchored at {} — skipping fold", top);
				return false;
			}
		}

		// Assign each boundary leaf a stable index (= position in the fused step's `underlyings` list).
		Map<CubeQueryStep, Integer> leafIndex = new LinkedHashMap<>();
		for (CubeQueryStep leaf : subgraph.boundary) {
			leafIndex.put(leaf, leafIndex.size());
		}

		// Post-order over internals: a step's children come before it in `steps`, so `inputSlots` always references
		// earlier slots. Plain reverse-BFS works because `internals` is in BFS order (parents-before-children).
		List<CubeQueryStep> postOrder = new ArrayList<>(subgraph.internals);
		Collections.reverse(postOrder);

		Map<CubeQueryStep, Integer> internalIndex = new HashMap<>();
		for (int i = 0; i < postOrder.size(); i++) {
			internalIndex.put(postOrder.get(i), i);
		}

		int numLeaves = leafIndex.size();
		List<CombineStep> steps = new ArrayList<>(postOrder.size());
		for (CubeQueryStep node : postOrder) {
			Combinator combinator = (Combinator) node.getMeasure();
			List<DefaultEdge> outEdges = new ArrayList<>(multigraph.outgoingEdgesOf(node));
			int[] inputSlots = new int[outEdges.size()];
			for (int i = 0; i < outEdges.size(); i++) {
				CubeQueryStep underlying = multigraph.getEdgeTarget(outEdges.get(i));
				if (leafIndex.containsKey(underlying)) {
					inputSlots[i] = leafIndex.get(underlying);
				} else {
					inputSlots[i] = numLeaves + internalIndex.get(underlying);
				}
			}
			steps.add(new CombineStep(combinator, inputSlots));
		}

		ComposedCombinationPlan plan = new ComposedCombinationPlan(numLeaves, steps);

		// Internals are iterated in BFS (top-down) order — top first, then its foldable children. We pass that order
		// verbatim to the name strategy so its output is stable regardless of HashMap iteration vagaries.
		List<Combinator> internalsTopDown =
				subgraph.internals.stream().map(s -> (Combinator) s.getMeasure()).collect(Collectors.toList());
		String fusedName = nameStrategy.name(internalsTopDown);
		Combinator.CombinatorBuilder fusedBuilder = Combinator.builder()
				.name(fusedName)
				.combinationKey(ComposedCombination.KEY)
				.combinationOption(ComposedCombination.K_PLAN, plan);
		for (CubeQueryStep leaf : subgraph.boundary) {
			fusedBuilder.underlying(leaf.getMeasure().getName());
		}
		CubeQueryStep fusedStep = CubeQueryStep.edit(top).measure(fusedBuilder.build()).build();

		// Snapshot EVERY consumer's outgoing-edge order BEFORE mutating so we can rebuild each with `top` substituted
		// by fusedStep. A naive `addEdge(consumer, fusedStep)` + later `removeVertex(top)` would append the new edge
		// to the END of the consumer's outgoing-edge set, breaking the positional contract the engine relies on to
		// map each outgoing edge to `Combinator#getUnderlyings()`. Top may have multiple consumers (each rewired to
		// the same fused step), so we capture the full set. See `DagFuserHelpers.replaceStepMeasure` for the same
		// pattern on the A-to-Combinator fusers.
		Set<CubeQueryStep> consumers = new LinkedHashSet<>();
		for (DefaultEdge in : multigraph.incomingEdgesOf(top)) {
			consumers.add(multigraph.getEdgeSource(in));
		}
		Map<CubeQueryStep, List<CubeQueryStep>> consumerOutgoingTargets = new LinkedHashMap<>();
		for (CubeQueryStep consumer : consumers) {
			List<CubeQueryStep> targets = new ArrayList<>();
			for (DefaultEdge e : multigraph.outgoingEdgesOf(consumer)) {
				targets.add(multigraph.getEdgeTarget(e));
			}
			consumerOutgoingTargets.put(consumer, targets);
		}

		// Mutate graphs: add the fused node + boundary edges, then drop every internal (which sweeps its edges).
		multigraph.addVertex(fusedStep);
		dag.addVertex(fusedStep);

		for (CubeQueryStep leaf : subgraph.boundary) {
			multigraph.addEdge(fusedStep, leaf);
			dag.addEdge(fusedStep, leaf);
		}
		for (CubeQueryStep node : subgraph.internals) {
			multigraph.removeVertex(node);
			dag.removeVertex(node);
		}

		// Rebuild each consumer's outgoing edges in the snapshotted order, substituting `top` → `fusedStep`. Use
		// `.equals()` not `==` : JGraphT canonicalises vertices via `.equals/.hashCode`, so a snapshotted target may
		// be a different instance than `top` even when they represent the same vertex.
		for (Map.Entry<CubeQueryStep, List<CubeQueryStep>> entry : consumerOutgoingTargets.entrySet()) {
			CubeQueryStep consumer = entry.getKey();
			for (DefaultEdge e : new ArrayList<>(multigraph.outgoingEdgesOf(consumer))) {
				multigraph.removeEdge(e);
			}
			for (DefaultEdge e : new ArrayList<>(dag.outgoingEdgesOf(consumer))) {
				dag.removeEdge(e);
			}
			for (CubeQueryStep target : entry.getValue()) {
				CubeQueryStep effective;
				if (top.equals(target)) {
					effective = fusedStep;
				} else {
					effective = target;
				}
				multigraph.addEdge(consumer, effective);
				dag.addEdge(consumer, effective);
			}
		}

		log.debug("Folded subgraph of {} internals + {} boundary leaves into {}",
				subgraph.internals.size(),
				subgraph.boundary.size(),
				fusedStep);
		return true;
	}

	protected record Subgraph(Set<CubeQueryStep> internals, Set<CubeQueryStep> boundary) {
	}
}
