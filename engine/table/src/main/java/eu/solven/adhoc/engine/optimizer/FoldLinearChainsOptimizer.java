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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.ComposedCombination;
import eu.solven.adhoc.model.measure.Combinator;
import lombok.extern.slf4j.Slf4j;

/**
 * Folds maximal linear chains of single-underlying {@link Combinator} steps into one {@link ComposedCombination}-backed
 * step, eliminating intermediate cuboid materialisations during query execution.
 *
 * <p>
 * A node is foldable iff: its measure is a {@link Combinator} with exactly one underlying, it has exactly one incoming
 * and one outgoing edge in the multigraph, it is not user-requested ({@code roots}) and it is not pre-loaded from cache
 * ({@code stepToValue}). A chain of {@code n >= minChainLength} consecutive foldable nodes is replaced by a single
 * fused step whose combination is the composition of the chain's combinations applied bottom-up (closest-to-leaf
 * first).
 *
 * <p>
 * Trade-off rationale: this optimizer adds a graph traversal at DAG-build time, but skips {@code n-1} cuboid
 * materialisations at execution time. Per {@code docs/optimization.md}, the asymmetric cost is accepted: trivial
 * queries pay a small fixed overhead, complex queries save proportionally to the chain length.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class FoldLinearChainsOptimizer implements IQueryStepsDagOptimizer {

	/** Default minimum chain length. Per {@code docs/optimization.md}, n=2 is the most aggressive useful threshold. */
	public static final int DEFAULT_MIN_CHAIN_LENGTH = 2;

	private final int minChainLength;

	public FoldLinearChainsOptimizer() {
		this(DEFAULT_MIN_CHAIN_LENGTH);
	}

	public FoldLinearChainsOptimizer(int minChainLength) {
		if (minChainLength < 2) {
			throw new IllegalArgumentException("minChainLength must be >= 2, got: " + minChainLength);
		}
		this.minChainLength = minChainLength;
	}

	@Override
	public void optimize(DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			IAdhocDag<CubeQueryStep> dag,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue) {
		// Snapshot of currently foldable nodes. Foldability is a function of the graph at this instant; we recompute
		// only at the start because each chain rewrite is self-contained — removing chain nodes does not affect
		// the foldability of any node outside the chain (mid-chain nodes had degree 1 in / 1 out, so they neither
		// added edges to nor consumed edges from anything outside the chain).
		Set<CubeQueryStep> foldable = new LinkedHashSet<>();
		for (CubeQueryStep step : multigraph.vertexSet()) {
			if (isFoldable(step, multigraph, roots, stepToValue)) {
				foldable.add(step);
			}
		}
		if (foldable.isEmpty()) {
			return;
		}

		Set<CubeQueryStep> processed = new HashSet<>();
		for (CubeQueryStep seed : foldable) {
			if (processed.contains(seed)) {
				continue;
			}
			List<CubeQueryStep> chain = expandMaximalChain(seed, foldable, multigraph);
			processed.addAll(chain);
			if (chain.size() < minChainLength) {
				continue;
			}
			rewriteChain(chain, multigraph, dag);
		}
	}

	/**
	 * A node is foldable when (a) its measure is a plain {@link Combinator} with one underlying, (b) it has exactly one
	 * incoming and one outgoing edge in the multigraph — so it is a "pure mid-chain transformer", and (c) it is neither
	 * user-requested nor pre-cached.
	 */
	protected boolean isFoldable(CubeQueryStep step,
			DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue) {
		if (!(step.getMeasure() instanceof Combinator combinator)) {
			return false;
		}
		if (combinator.getUnderlyings().size() != 1) {
			return false;
		}
		if (multigraph.incomingEdgesOf(step).size() != 1) {
			return false;
		}
		if (multigraph.outgoingEdgesOf(step).size() != 1) {
			return false;
		}
		if (roots.contains(step)) {
			return false;
		}
		if (stepToValue.containsKey(step)) {
			return false;
		}
		return true;
	}

	/**
	 * Walk up from {@code seed} (toward consumers) and down (toward underlyings) while neighbours remain foldable,
	 * returning the maximal chain ordered top-down: {@code [topmost foldable, ..., bottommost foldable]}.
	 */
	protected List<CubeQueryStep> expandMaximalChain(CubeQueryStep seed,
			Set<CubeQueryStep> foldable,
			DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph) {
		// Walk UP (toward consumers): follow the single incoming edge while the neighbour is still foldable.
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

		// Walk DOWN from `top` (toward underlyings), accumulating the chain.
		List<CubeQueryStep> chain = new ArrayList<>();
		CubeQueryStep cursor = top;
		while (true) {
			chain.add(cursor);
			Set<DefaultEdge> out = multigraph.outgoingEdgesOf(cursor);
			if (out.size() != 1) {
				break;
			}
			CubeQueryStep child = multigraph.getEdgeTarget(out.iterator().next());
			if (!foldable.contains(child)) {
				break;
			}
			cursor = child;
		}
		return chain;
	}

	/**
	 * Replace {@code chain} (ordered top-down) with a single fused step. The fused step inherits the topmost step's
	 * filter / groupBy / customMarker / options; its measure is a {@link Combinator} whose combination is a
	 * {@link ComposedCombination} of the chain's measures applied bottom-up (closest-to-leaf first).
	 */
	protected void rewriteChain(List<CubeQueryStep> chain,
			DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			IAdhocDag<CubeQueryStep> dag) {
		CubeQueryStep top = chain.get(0);
		CubeQueryStep bottom = chain.get(chain.size() - 1);

		// Skip the fold if any chain element disagrees with the topmost on filter / groupBy / customMarker. In the
		// expected Combinator shape every element matches its underlying on these dimensions, but a customised
		// IMeasureQueryStep could break the assumption; bail rather than silently produce wrong results.
		for (CubeQueryStep step : chain) {
			if (!step.getFilter().equals(top.getFilter()) || !step.getGroupBy().equals(top.getGroupBy())
					|| !Objects.equals(step.getCustomMarker(), top.getCustomMarker())) {
				log.debug("Heterogeneous filter/groupBy/customMarker in chain — skipping fold for {}", chain);
				return;
			}
		}

		// Single incoming on top → consumer. Single outgoing on bottom → underlying step.
		CubeQueryStep consumer = multigraph.getEdgeSource(multigraph.incomingEdgesOf(top).iterator().next());
		CubeQueryStep underlying = multigraph.getEdgeTarget(multigraph.outgoingEdgesOf(bottom).iterator().next());

		// Compose bottom-up: the value-reading side runs first.
		List<Combinator> chainMeasures = new ArrayList<>(chain.size());
		for (int i = chain.size() - 1; i >= 0; i--) {
			chainMeasures.add((Combinator) chain.get(i).getMeasure());
		}

		String fusedName = chain.stream().map(s -> s.getMeasure().getName()).collect(Collectors.joining(" ∘ "));
		String underlyingMeasureName = ((Combinator) bottom.getMeasure()).getUnderlyings().get(0);

		Combinator fusedMeasure = Combinator.builder()
				.name(fusedName)
				.underlying(underlyingMeasureName)
				.combinationKey(ComposedCombination.class.getName())
				.combinationOption(ComposedCombination.K_CHAIN, ImmutableList.copyOf(chainMeasures))
				.build();

		CubeQueryStep fusedStep = CubeQueryStep.edit(top).measure(fusedMeasure).build();

		// Mutate graphs: add the fused node + the two boundary edges, then drop the chain (which sweeps its edges).
		multigraph.addVertex(fusedStep);
		dag.addVertex(fusedStep);
		multigraph.addEdge(consumer, fusedStep);
		dag.addEdge(consumer, fusedStep);
		multigraph.addEdge(fusedStep, underlying);
		dag.addEdge(fusedStep, underlying);

		for (CubeQueryStep step : chain) {
			multigraph.removeVertex(step);
			dag.removeVertex(step);
		}

		log.debug("Folded chain of {} into single step {}", chain.size(), fusedStep);
	}
}
