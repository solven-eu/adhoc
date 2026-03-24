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
package eu.solven.adhoc.engine.tabular.optimizer;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;

/**
 * A directed acyclic graph with unlabeled ({@link DefaultEdge}) edges, representing dependency relationships between
 * query steps. The {@link DefaultEdge} type parameter is an implementation detail of JGraphT; this interface hides it
 * so call sites only need to know the vertex type {@code T}.
 *
 * <p>
 * <b>Edge direction convention</b>: an edge goes from the <em>dependant</em> (the induced node) to its
 * <em>dependency</em> (the inducer). In other words, if node A depends on node B, there is an edge {@code A → B}.
 * Execution order is therefore the reverse topological order: inducers (dependencies) are computed before the nodes
 * that depend on them.
 *
 * <p>
 * The full {@link Graph} contract (add/remove vertices and edges, degree queries, neighbour traversal, etc.) is
 * inherited. Implementations must guarantee acyclicity: {@link Graph#addEdge} must throw
 * {@link IllegalArgumentException} when the new edge would introduce a cycle.
 *
 * @param <T>
 *            the vertex type
 * @author Benoit Lacelle
 */
public interface IAdhocDag<T> extends Graph<T, DefaultEdge> {
}
