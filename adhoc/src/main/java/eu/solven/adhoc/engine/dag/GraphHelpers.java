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
package eu.solven.adhoc.engine.dag;

import org.jgrapht.Graphs;

import com.google.common.collect.ImmutableSet;

import lombok.experimental.UtilityClass;

/**
 * Helps working with JGraph4T
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class GraphHelpers {
	@SuppressWarnings({ "rawtypes", "PMD.AvoidFieldNameMatchingMethodName" })
	private static final IAdhocDag EMPTY = immutable(makeGraph());

	@SuppressWarnings("unchecked")
	public static <T> IAdhocDag<T> empty() {
		return EMPTY;
	}

	public static <T> IAdhocDag<T> makeGraph() {
		return new AdhocDag<>();
	}

	public static <T> IAdhocDag<T> immutable(IAdhocDag<T> dag) {
		return new AdhocImmutableDag<>(dag);
	}

	/**
	 * 
	 * @param <T>
	 * @param graph
	 * @return the set of inducers, i.e. nodes which has no children
	 */
	// relates with `Graphs.vertexHasSuccessors`
	public static <T> ImmutableSet<T> getInducers(IAdhocDag<T> graph) {
		return graph.vertexSet().stream().filter(s -> graph.outDegreeOf(s) == 0).collect(ImmutableSet.toImmutableSet());
	}

	public static <T> ImmutableSet<T> getInduceds(IAdhocDag<T> graph) {
		return graph.vertexSet().stream().filter(s -> graph.outDegreeOf(s) != 0).collect(ImmutableSet.toImmutableSet());
	}

	public static <T> ImmutableSet<T> getInduced(IAdhocDag<T> graph, T step) {
		return graph.incomingEdgesOf(step).stream().map(graph::getEdgeSource).collect(ImmutableSet.toImmutableSet());
	}

	// relates with `Graphs.vertexHasSuccessors`
	public static <T> ImmutableSet<T> getRoots(IAdhocDag<T> graph) {
		return graph.vertexSet().stream().filter(s -> graph.inDegreeOf(s) == 0).collect(ImmutableSet.toImmutableSet());
	}

	public static <T> IAdhocDag<T> copy(IAdhocDag<T> graph) {
		IAdhocDag<T> copied = makeGraph();

		Graphs.addGraph(copied, graph);

		return copied;

	}

}
