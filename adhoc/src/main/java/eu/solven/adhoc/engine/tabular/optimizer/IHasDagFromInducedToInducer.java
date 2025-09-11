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

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;

/**
 * For data-structure holding a tree-like representing tasks and dependent tasks.
 * 
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IHasDagFromInducedToInducer {
	/**
	 * This DAG has edges from the queried/output/induced to the underlyings/input/inducer.
	 * 
	 * @return
	 */
	DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> getInducedToInducer();

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
}
