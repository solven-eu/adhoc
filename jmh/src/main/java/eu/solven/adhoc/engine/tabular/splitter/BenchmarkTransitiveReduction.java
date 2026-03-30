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

import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

import org.jgrapht.alg.TransitiveReduction;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import eu.solven.adhoc.jgrapht.alg.TransitiveReductionV2;

/**
 * Benchmarks {@link TransitiveReductionV2} on a simple linear chain a₀ → a₁ → … → aₙ.
 *
 * <p>
 * A chain is the worst case for the naïve O(n²) removal loop: after path-matrix expansion every node can reach every
 * later node, so the reduced matrix has exactly n-1 set bits while the naïve loop calls {@code getEdge} for O(n²)
 * pairs. The optimised loop iterates only the original edges and therefore does O(n) work here.
 * </p>
 *
 * @author Benoit Lacelle
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
@SuppressWarnings("checkstyle:MagicNumber")
public class BenchmarkTransitiveReduction {

	@Param({ "100", "500", "1000" })
	int nodeCount;

	/**
	 * Builds a fresh linear-chain graph for each invocation so that each benchmark call starts from the same state.
	 */
	private SimpleDirectedGraph<Integer, DefaultEdge> buildChain() {
		SimpleDirectedGraph<Integer, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);
		for (int i = 0; i < nodeCount; i++) {
			graph.addVertex(i);
		}
		for (int i = 0; i < nodeCount - 1; i++) {
			graph.addEdge(i, i + 1);
		}
		return graph;
	}

	private SimpleDirectedGraph<LocalDate, DefaultEdge> buildChain_LocalDate() {
		SimpleDirectedGraph<LocalDate, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);
		for (int i = 0; i < nodeCount; i++) {
			graph.addVertex(LocalDate.of(i, 1, 1));
		}
		for (int i = 0; i < nodeCount - 1; i++) {
			graph.addEdge(LocalDate.of(i, 1, 1), LocalDate.of(i + 1, 1, 1));
		}
		return graph;
	}

	/**
	 * Measures end-to-end {@link TransitiveReductionV2#reduce} time on a linear chain. The chain has n-1 edges; after
	 * reduction it still has n-1 edges (every hop is necessary), so the benchmark exercises the full algorithm without
	 * removing a single edge — stressing the removal-loop scan cost rather than actual graph mutations.
	 */
	@Benchmark
	public int reduceChain_v2() {
		SimpleDirectedGraph<?, DefaultEdge> graph = buildChain();
		TransitiveReductionV2.INSTANCE.reduce(graph);
		return graph.edgeSet().size();
	}

	@Benchmark
	public int reduceChain_LocalDate_v2() {
		SimpleDirectedGraph<?, DefaultEdge> graph = buildChain_LocalDate();
		TransitiveReductionV2.INSTANCE.reduce(graph);
		return graph.edgeSet().size();
	}

	/**
	 * Same scenario using the original {@link TransitiveReduction} implementation, as a baseline for comparison against
	 * {@link #reduceChain_v2()}.
	 */
	@Benchmark
	public int reduceChain_original() {
		SimpleDirectedGraph<?, DefaultEdge> graph = buildChain();
		TransitiveReduction.INSTANCE.reduce(graph);
		return graph.edgeSet().size();
	}

	@Benchmark
	public int reduceChain_LocalDate_original() {
		SimpleDirectedGraph<?, DefaultEdge> graph = buildChain_LocalDate();
		TransitiveReduction.INSTANCE.reduce(graph);
		return graph.edgeSet().size();
	}
}
