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
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Partitionor;
import lombok.extern.slf4j.Slf4j;

/**
 * Rewrites {@link Partitionor}-measure steps as equivalent {@link Combinator}-measure steps when the step's groupBy
 * already covers the Partitionor's own groupBy. See {@code docs/optimization.md} for context.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class PartitionorToCombinatorOptimizer implements IQueryStepsDagOptimizer {

	@Override
	public void optimize(DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			IAdhocDag<CubeQueryStep> dag,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue) {
		List<CubeQueryStep> candidates = new ArrayList<>(multigraph.vertexSet());
		for (CubeQueryStep step : candidates) {
			if (!(step.getMeasure() instanceof Partitionor partitionor)) {
				continue;
			}
			if (roots.contains(step)) {
				continue;
			}
			if (stepToValue.containsKey(step)) {
				continue;
			}
			if (!step.getGroupBy().getSortedColumns().containsAll(partitionor.getGroupBy().getSortedColumns())) {
				continue;
			}

			Combinator.CombinatorBuilder builder = Combinator.builder()
					.name(partitionor.getName())
					.combinationKey(partitionor.getCombinationKey())
					.tags(partitionor.getTags());
			for (String underlying : partitionor.getUnderlyings()) {
				builder.underlying(underlying);
			}
			for (Map.Entry<String, ?> opt : partitionor.getCombinationOptions().entrySet()) {
				builder.combinationOption(opt.getKey(), opt.getValue());
			}
			Combinator replacement = builder.build();
			CubeQueryStep newStep = CubeQueryStep.edit(step).measure(replacement).build();

			multigraph.addVertex(newStep);
			dag.addVertex(newStep);

			// Snapshot each consumer's outgoing-edge order BEFORE we remove the old vertex. The engine reads
			// `multigraph.outgoingEdgesOf(consumer)` in insertion order and maps the i-th edge to
			// `Combinator.underlyings[i]` at evaluation time — so if we just append a new edge after deleting the
			// old one, the consumer's underlying order shifts and the combination receives values at the wrong
			// positions. We rebuild every consumer's full outgoing-edge list with the old step substituted by the
			// new one, preserving each position.
			List<CubeQueryStep> consumers = new ArrayList<>();
			Map<CubeQueryStep, List<CubeQueryStep>> consumerOutgoingOrder = new LinkedHashMap<>();
			for (DefaultEdge in : multigraph.incomingEdgesOf(step)) {
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

			// Snapshot the old step's outgoing edges (in order) — these get re-attached to newStep verbatim.
			List<CubeQueryStep> oldOutgoing = new ArrayList<>();
			for (DefaultEdge outEdge : multigraph.outgoingEdgesOf(step)) {
				oldOutgoing.add(multigraph.getEdgeTarget(outEdge));
			}

			// Wire newStep's outgoing edges (order preserved).
			for (CubeQueryStep tgt : oldOutgoing) {
				multigraph.addEdge(newStep, tgt);
				dag.addEdge(newStep, tgt);
			}

			// Drop the old step (removes all its in/out edges, including each consumer's outgoing-to-old).
			multigraph.removeVertex(step);
			dag.removeVertex(step);

			// Rebuild each consumer's outgoing edges in original order, substituting old → new. For each consumer
			// we remove its current outgoing edges (the ones not touching `step`, which the removeVertex above
			// left alone) and re-add them in the saved order.
			for (CubeQueryStep consumer : consumers) {
				List<DefaultEdge> survivingOut = new ArrayList<>(multigraph.outgoingEdgesOf(consumer));
				for (DefaultEdge e : survivingOut) {
					multigraph.removeEdge(e);
				}
				List<DefaultEdge> survivingDagOut = new ArrayList<>(dag.outgoingEdgesOf(consumer));
				for (DefaultEdge e : survivingDagOut) {
					dag.removeEdge(e);
				}
				for (CubeQueryStep tgt : consumerOutgoingOrder.get(consumer)) {
					// JGraphT canonicalises vertices via .equals/.hashCode (not by reference identity), so a `tgt`
					// captured from the graph may be a different instance than `step` even when they represent the
					// same vertex. Use .equals() not == to recognise the step being rewritten.
					CubeQueryStep effective = step.equals(tgt) ? newStep : tgt;
					multigraph.addEdge(consumer, effective);
					dag.addEdge(consumer, effective);
				}
			}

			log.debug("Rewrote Partitionor step {} as Combinator", partitionor.getName());
		}
	}
}
