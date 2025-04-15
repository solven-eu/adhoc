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
package eu.solven.adhoc.dag;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import eu.solven.adhoc.dag.step.AdhocQueryStep;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * Holds the details about the queryPlan as a DAG.
 * 
 * @author Benoit Lacelle
 * @see QueryStepsDagBuilder
 */
@Value
@Builder
public class QueryStepsDag {
	// The DAG of a given IAdhocQuery, from queried to aggregators
	@NonNull
	DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag;

	// The multigraph of a given IAdhocQuery, from queried to aggregators, accepting a queriedStep to query multiple
	// times the same underlyingStep
	@NonNull
	DirectedMultigraph<AdhocQueryStep, DefaultEdge> multigraph;

	// We keep a separate list of queried steps, as some queried may not be roots in the DAG (e.g. when the query
	// requests both a measure and one of its underlying)
	@NonNull
	Set<AdhocQueryStep> queried;

	@NonNull
	@Default
	Map<AdhocQueryStep, SizeAndDuration> stepToCost = new ConcurrentHashMap<>();

	public List<AdhocQueryStep> underlyingSteps(AdhocQueryStep queryStep) {
		if (queryStep.getMeasure() instanceof ReferencedMeasure refMeasure) {
			throw new IllegalArgumentException(
					"The measure must be explicit: %s".formatted(PepperLogHelper.getObjectAndClass(refMeasure)));
		}

		// Given EdgeSetFactory, this Set is backed by an ArrayList, and ordering is then maintained
		Set<DefaultEdge> outgoingEdges = multigraph.outgoingEdgesOf(queryStep);
		// underlyingSteps are on the opposite of queryStep edges
		return outgoingEdges.stream().map(edge -> Graphs.getOppositeVertex(dag, edge, queryStep)).toList();
	}

	// TODO Enable returning the Set of queyrSteps which can be executed concurrently
	public Iterator<AdhocQueryStep> fromAggregatesToQueried() {
		// https://stackoverflow.com/questions/69183360/traversal-of-edgereversedgraph
		EdgeReversedGraph<AdhocQueryStep, DefaultEdge> fromAggregatesToQueried = new EdgeReversedGraph<>(dag);

		// https://en.wikipedia.org/wiki/Topological_sorting
		// TopologicalOrder guarantees processing a vertex after dependent vertices are
		// done.
		return new TopologicalOrderIterator<>(fromAggregatesToQueried);
	}

	public void registerExecutionFeedback(AdhocQueryStep queryStep, SizeAndDuration sizeAndDuration) {
		stepToCost.put(queryStep, sizeAndDuration);
	}
}
