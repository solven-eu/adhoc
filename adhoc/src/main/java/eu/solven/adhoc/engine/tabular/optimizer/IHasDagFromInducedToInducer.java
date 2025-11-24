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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;

/**
 * For data-structure holding a tree-like representing tasks and dependent tasks.
 * 
 * @author Benoit Lacelle
 */
public interface IHasDagFromInducedToInducer {
	/**
	 * 
	 * @return the {@link CubeQueryStep} which are explicitly requested. In the DAG, some of these steps may have
	 *         parents.
	 */
	ImmutableSet<CubeQueryStep> getExplicits();

	/**
	 * This DAG has edges from the queried/output/induced to the underlyings/input/inducer.
	 * 
	 * @return
	 */
	DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> getInducedToInducer();

	Map<CubeQueryStep, SizeAndDuration> getStepToCost();

	/**
	 * 
	 * @return a {@link Iterator} from {@link Aggregator} up to queried nodes, guaranteeing a {@link CubeQueryStep} is
	 *         encountered strictly after its underlyings.
	 */

	default Iterator<CubeQueryStep> iteratorFromInducerToInduced() {
		// https://stackoverflow.com/questions/69183360/traversal-of-edgereversedgraph
		EdgeReversedGraph<CubeQueryStep, DefaultEdge> fromAggregatesToQueried =
				new EdgeReversedGraph<>(getInducedToInducer());

		return new TopologicalOrderIterator<>(fromAggregatesToQueried);
	}

	default List<CubeQueryStep> getInducers(CubeQueryStep induced) {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = getInducedToInducer();
		return dag.outgoingEdgesOf(induced).stream().map(dag::getEdgeTarget).toList();
	}

	// Holds the TableQuery which can not be implicitly evaluated, and needs to be executed directly
	default ImmutableSet<CubeQueryStep> getInducers() {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer = getInducedToInducer();

		// relates with `Graphs.vertexHasSuccessors`
		return inducedToInducer.vertexSet()
				.stream()
				.filter(s -> inducedToInducer.outDegreeOf(s) == 0)
				.collect(ImmutableSet.toImmutableSet());
	}

	// Holds the TableQuery which can be evaluated implicitly from underlyings
	default ImmutableSet<CubeQueryStep> getInduceds() {
		return ImmutableSet.copyOf(Sets.difference(getInducedToInducer().vertexSet(), getInducers()));
	}

	/**
	 * 
	 * @return the {@link Set} of roots. This is a subset of explicit steps, as some additional explicit steps may be in
	 *         the middle of the DAG, as (intermediate) inducer of other roots.
	 */
	default ImmutableSet<CubeQueryStep> getRoots() {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer = getInducedToInducer();

		// relates with `Graphs.vertexHasSuccessors`
		return inducedToInducer.vertexSet()
				.stream()
				.filter(s -> inducedToInducer.inDegreeOf(s) == 0)
				.collect(ImmutableSet.toImmutableSet());
	}
}
