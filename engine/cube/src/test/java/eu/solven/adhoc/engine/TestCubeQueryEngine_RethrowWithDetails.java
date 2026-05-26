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
package eu.solven.adhoc.engine;

import java.util.List;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.dag.AdhocDag;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.model.measure.Aggregator;

/**
 * Focused unit tests for {@link CubeQueryEngine#rethrowWithDetails}: standalone, no in-memory cube / table set-up.
 * Builds {@link QueryStepsDag} instances by hand to exercise both branches of the size-driven path-from-root render.
 */
public class TestCubeQueryEngine_RethrowWithDetails {

	/**
	 * Above the 1024-edge threshold, {@code rethrowWithDetails} skips the expensive {@code JohnsonShortestPaths} and
	 * falls back to a naive first-incoming-edge walk from the failing step up to a root. The exception message still
	 * carries a "Path from root:" trace — just an arbitrary one rather than the shortest — which is the property this
	 * test pins.
	 */
	@Test
	public void testLargeDagFallsBackToNaivePath() {
		// 1100 vertices, 1099 edges — comfortably above the 1024-edge threshold.
		int chainLength = 1100;
		IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
		DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph = new DirectedMultigraph<>(DefaultEdge.class);
		List<CubeQueryStep> chain = IntStream.range(0, chainLength)
				.mapToObj(i -> CubeQueryStep.builder().measure(Aggregator.sum("m" + i)).build())
				.toList();
		chain.forEach(step -> {
			dag.addVertex(step);
			multigraph.addVertex(step);
		});
		for (int i = 0; i < chainLength - 1; i++) {
			// Edge from the consumer (i) down to its inducer (i + 1), matching the IAdhocDag induced→inducer convention.
			dag.addEdge(chain.get(i), chain.get(i + 1));
			multigraph.addEdge(chain.get(i), chain.get(i + 1));
		}

		QueryStepsDag qsd = QueryStepsDag.builder()
				.inducedToInducer(dag)
				.multigraph(multigraph)
				.explicit(chain.get(0))
				.build();

		CubeQueryEngine engine = CubeQueryEngine.builder().build();
		IllegalStateException thrown =
				engine.rethrowWithDetails(chain.get(chainLength - 1), qsd, new RuntimeException("boom"));

		Assertions.assertThat(thrown.getMessage())
				.contains("Issue computing columns for:")
				// The naive branch prints the edge count to make the size-driven fallback explicit.
				.contains("#steps=" + (chainLength - 1))
				.contains("Path from root:")
				// First and last steps of the walk — full root-to-failing-leaf trace present in the rendered path.
				.contains("measure=m0:SUM(m0)")
				.contains("measure=m" + (chainLength - 1) + ":SUM(m" + (chainLength - 1) + ")");
	}
}
