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
package eu.solven.adhoc.engine;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jgrapht.GraphIterables;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.DirectedMultigraph;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.IHasDagFromInducedToInducer;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds the details about the queryPlan as a DAG.
 * 
 * @author Benoit Lacelle
 * @see QueryStepsDagBuilder
 */
@Value
@Builder
@Slf4j
public class QueryStepsDag implements ISinkExecutionFeedback, IHasDagFromInducedToInducer {
	// The DAG of a given IAdhocQuery, from queried to aggregators. It does not accept multiple times the same edge
	// (e.g. a ratio and a filter leading to same numerator and denominator).
	@NonNull
	DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer;

	// The multigraph of a given IAdhocQuery, from queried to aggregators, accepting a queriedStep to query multiple
	// times the same underlyingStep
	// (e.g. a ratio and a filter leading to same numerator and denominator).
	@NonNull
	DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph;

	// We keep a separate list of queried steps, as some queried may not be roots in the DAG (e.g. when the query
	// requests both a measure and one of its underlying)
	@NonNull
	@Singular
	ImmutableSet<CubeQueryStep> explicits;

	@NonNull
	@Default
	Map<CubeQueryStep, SizeAndDuration> stepToCost = new ConcurrentHashMap<>();

	@NonNull
	@Singular
	ImmutableMap<CubeQueryStep, ISliceToValue> stepToValues;

	public List<CubeQueryStep> underlyingSteps(CubeQueryStep queryStep) {
		if (queryStep.getMeasure() instanceof ReferencedMeasure refMeasure) {
			throw new IllegalArgumentException(
					"The measure must be explicit: %s".formatted(PepperLogHelper.getObjectAndClass(refMeasure)));
		}

		// Given EdgeSetFactory, this Set is backed by an ArrayList, and ordering is then maintained
		Set<DefaultEdge> outgoingEdges = multigraph.outgoingEdgesOf(queryStep);
		// underlyingSteps are on the opposite of queryStep edges
		return outgoingEdges.stream().map(edge -> Graphs.getOppositeVertex(inducedToInducer, edge, queryStep)).toList();
	}

	@Override
	public void registerExecutionFeedback(CubeQueryStep queryStep, SizeAndDuration sizeAndDuration) {
		stepToCost.put(queryStep, sizeAndDuration);
	}

	@Override
	public DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> getInducedToInducer() {
		return inducedToInducer;
	}

	@Override
	public long edgeCount() {
		return inducedToInducer.iterables().edgeCount();
	}

}
