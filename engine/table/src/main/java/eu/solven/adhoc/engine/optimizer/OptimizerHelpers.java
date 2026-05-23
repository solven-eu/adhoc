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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.model.measure.IMeasure;

/**
 * Shared utilities for {@link IQueryStepsDagOptimizer} implementations.
 *
 * @author Benoit Lacelle
 */
final class OptimizerHelpers {

	private OptimizerHelpers() {
	}

	/**
	 * Replace the measure carried by {@code oldStep} with {@code newMeasure}, preserving every consumer's outgoing-edge
	 * order. The engine maps {@code multigraph.outgoingEdgesOf(consumer)} iteration order to
	 * {@code Combinator.underlyings[i]} positionally, so a naive {@code removeVertex(oldStep)} + later
	 * {@code addEdge(consumer, newStep)} would silently shift the order and corrupt the combination's input order.
	 *
	 * @param multigraph
	 *            mutated in place — {@code oldStep} is removed and a new vertex carrying {@code newMeasure} is added.
	 * @param dag
	 *            mutated in place to stay in sync with {@code multigraph}.
	 * @param oldStep
	 *            the step to rewrite. Must be a vertex of {@code multigraph}.
	 * @param newMeasure
	 *            the replacement measure.
	 * @return the new {@link CubeQueryStep} that replaced {@code oldStep}.
	 */
	static CubeQueryStep replaceStepMeasure(DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			IAdhocDag<CubeQueryStep> dag,
			CubeQueryStep oldStep,
			IMeasure newMeasure) {
		CubeQueryStep newStep = CubeQueryStep.edit(oldStep).measure(newMeasure).build();
		multigraph.addVertex(newStep);
		dag.addVertex(newStep);

		// Snapshot every consumer's outgoing-edge order BEFORE removing the old vertex. We'll rebuild each
		// consumer's outgoing edges in this saved order with `oldStep` substituted by `newStep`, preserving the
		// positional contract.
		List<CubeQueryStep> consumers = new ArrayList<>();
		Map<CubeQueryStep, List<CubeQueryStep>> consumerOutgoingOrder = new LinkedHashMap<>();
		for (DefaultEdge in : multigraph.incomingEdgesOf(oldStep)) {
			CubeQueryStep src = multigraph.getEdgeSource(in);
			if (consumerOutgoingOrder.containsKey(src)) {
				continue;
			}
			consumers.add(src);
			List<CubeQueryStep> targets = new ArrayList<>();
			for (DefaultEdge outEdge : multigraph.outgoingEdgesOf(src)) {
				targets.add(multigraph.getEdgeTarget(outEdge));
			}
			consumerOutgoingOrder.put(src, targets);
		}

		// Re-attach the old step's outgoing edges (in order) to the new step verbatim.
		List<CubeQueryStep> oldOutgoing = new ArrayList<>();
		for (DefaultEdge outEdge : multigraph.outgoingEdgesOf(oldStep)) {
			oldOutgoing.add(multigraph.getEdgeTarget(outEdge));
		}
		for (CubeQueryStep tgt : oldOutgoing) {
			multigraph.addEdge(newStep, tgt);
			dag.addEdge(newStep, tgt);
		}

		// Drop the old step (sweeps its remaining edges, including each consumer's outgoing-to-old).
		multigraph.removeVertex(oldStep);
		dag.removeVertex(oldStep);

		// Rebuild each consumer's outgoing edges in the snapshotted order, with oldStep → newStep. Use .equals()
		// not == : JGraphT canonicalises vertices via .equals/.hashCode, so `tgt` may be a different instance
		// than `oldStep` even when they represent the same vertex.
		for (CubeQueryStep consumer : consumers) {
			for (DefaultEdge e : new ArrayList<>(multigraph.outgoingEdgesOf(consumer))) {
				multigraph.removeEdge(e);
			}
			for (DefaultEdge e : new ArrayList<>(dag.outgoingEdgesOf(consumer))) {
				dag.removeEdge(e);
			}
			for (CubeQueryStep tgt : consumerOutgoingOrder.get(consumer)) {
				CubeQueryStep effective;
				if (oldStep.equals(tgt)) {
					effective = newStep;
				} else {
					effective = tgt;
				}
				multigraph.addEdge(consumer, effective);
				dag.addEdge(consumer, effective);
			}
		}

		return newStep;
	}
}
