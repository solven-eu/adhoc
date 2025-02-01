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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;

public class QueryStepsDagsBuilder {
	final IAdhocQuery query;

	final Set<AdhocQueryStep> roots = new HashSet<>();

	final DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> queryDag = new DirectedAcyclicGraph<>(DefaultEdge.class);

	// Holds the querySteps which underlying steps are pending for processing
	final LinkedList<AdhocQueryStep> leftOvers = new LinkedList<>();

	public QueryStepsDagsBuilder(IAdhocQuery query) {
		this.query = query;
	}

	public void addRoot(IMeasure queriedMeasure) {
		AdhocQueryStep rootStep = AdhocQueryStep.builder()
				.filter(query.getFilter())
				.groupBy(query.getGroupBy())
				.measure(queriedMeasure)
				.customMarker(query.getCustomMarker())
				.debug(query.isDebug())
				.build();

		roots.add(rootStep);
		queryDag.addVertex(rootStep);
		leftOvers.add(rootStep);
	}

	public boolean hasLeftovers() {
		return !leftOvers.isEmpty();
	}

	public AdhocQueryStep pollLeftover() {
		return leftOvers.poll();
	}

	public void addEdge(AdhocQueryStep adhocSubQuery, AdhocQueryStep underlyingStep) {
		queryDag.addVertex(underlyingStep);
		queryDag.addEdge(adhocSubQuery, underlyingStep);

		leftOvers.add(underlyingStep);
	}

	public void sanityChecks() {
		// sanity check
		queryDag.vertexSet().forEach(step -> {
			if (step.getMeasure() instanceof ReferencedMeasure) {
				throw new IllegalStateException("The DAG must not rely on ReferencedMeasure");
			}
		});
	}

	public DagHolder getQueryDag() {
		return DagHolder.builder().dag(queryDag).queried(roots).build();
	}
}
