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
package eu.solven.adhoc.engine.tabular.splitter;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.jgrapht.alg.TransitiveReductionV2;

/**
 * Tests for {@link TransitiveReductionV2}.
 */
public class TestTransitiveReduction {

	private SimpleDirectedGraph<Integer, DefaultEdge> newGraph() {
		return new SimpleDirectedGraph<>(DefaultEdge.class);
	}

	/**
	 * Linear chain: 0 → 1 → 2. No edge is redundant; the graph must be unchanged.
	 */
	@Test
	public void testChain_noEdgeRemoved() {
		SimpleDirectedGraph<Integer, DefaultEdge> g = newGraph();
		g.addVertex(0);
		g.addVertex(1);
		g.addVertex(2);
		g.addEdge(0, 1);
		g.addEdge(1, 2);

		TransitiveReductionV2.INSTANCE.reduce(g);

		Assertions.assertThat(g.edgeSet()).hasSize(2);
		Assertions.assertThat(g.containsEdge(0, 1)).isTrue();
		Assertions.assertThat(g.containsEdge(1, 2)).isTrue();
	}

	/**
	 * Diamond with shortcut: 0→1, 1→2, 0→2. The direct edge 0→2 is redundant and must be removed.
	 */
	@Test
	public void testDiamond_redundantEdgeRemoved() {
		SimpleDirectedGraph<Integer, DefaultEdge> g = newGraph();
		g.addVertex(0);
		g.addVertex(1);
		g.addVertex(2);
		g.addEdge(0, 1);
		g.addEdge(1, 2);
		g.addEdge(0, 2);

		TransitiveReductionV2.INSTANCE.reduce(g);

		Assertions.assertThat(g.edgeSet()).hasSize(2);
		Assertions.assertThat(g.containsEdge(0, 1)).isTrue();
		Assertions.assertThat(g.containsEdge(1, 2)).isTrue();
		Assertions.assertThat(g.containsEdge(0, 2)).isFalse();
	}

	/**
	 * Long chain (1 000 nodes): 0→1→2→…→999. All edges are necessary; none must be removed. This exercises the
	 * optimised removal loop which must avoid O(n²) getEdge calls.
	 */
	@Test
	public void testLongChain_noEdgeRemoved() {
		int n = 1000;
		SimpleDirectedGraph<Integer, DefaultEdge> g = newGraph();
		for (int i = 0; i < n; i++) {
			g.addVertex(i);
		}
		for (int i = 0; i < n - 1; i++) {
			g.addEdge(i, i + 1);
		}

		TransitiveReductionV2.INSTANCE.reduce(g);

		Assertions.assertThat(g.edgeSet()).hasSize(n - 1);
	}

	/**
	 * Long chain with a skip edge: 0→1→…→999 plus 0→999. The skip edge is redundant and must be removed; all chain
	 * edges must survive.
	 */
	@Test
	public void testLongChain_skipEdgeRemoved() {
		int n = 100;
		SimpleDirectedGraph<Integer, DefaultEdge> g = newGraph();
		for (int i = 0; i < n; i++) {
			g.addVertex(i);
		}
		for (int i = 0; i < n - 1; i++) {
			g.addEdge(i, i + 1);
		}
		g.addEdge(0, n - 1);

		TransitiveReductionV2.INSTANCE.reduce(g);

		Assertions.assertThat(g.edgeSet()).hasSize(n - 1);
		Assertions.assertThat(g.containsEdge(0, n - 1)).isFalse();
	}

	/** Empty graph must not throw. */
	@Test
	public void testEmpty() {
		SimpleDirectedGraph<Integer, DefaultEdge> g = newGraph();
		TransitiveReductionV2.INSTANCE.reduce(g);
		Assertions.assertThat(g.edgeSet()).isEmpty();
	}
}
