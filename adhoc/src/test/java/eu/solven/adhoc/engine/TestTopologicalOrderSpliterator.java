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

import java.util.Spliterator;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.CubeQueryStep;

public class TestTopologicalOrderSpliterator {
	@Test
	public void testTopologicalOrderSpliterator_empty() {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag =
				DirectedAcyclicGraph.<CubeQueryStep, DefaultEdge>createBuilder(DefaultEdge.class).build();
		Stream<CubeQueryStep> spliterator = TopologicalOrderSpliterator.fromDAG(dag);

		Assertions.assertThat(spliterator.isParallel()).isFalse();
	}

	@Test
	public void testTopologicalOrderSpliterator_single() {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag =
				DirectedAcyclicGraph.<CubeQueryStep, DefaultEdge>createBuilder(DefaultEdge.class).build();

		CubeQueryStep source = Mockito.mock(CubeQueryStep.class);

		dag.addVertex(source);

		Spliterator<CubeQueryStep> spliterator = TopologicalOrderSpliterator.makeSpliterator(dag);

		Assertions.assertThat(spliterator.trySplit()).isNull();
	}

	@Test
	public void testTopologicalOrderSpliterator_twoLinked() {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag =
				DirectedAcyclicGraph.<CubeQueryStep, DefaultEdge>createBuilder(DefaultEdge.class).build();

		CubeQueryStep source = Mockito.mock(CubeQueryStep.class);
		CubeQueryStep target = Mockito.mock(CubeQueryStep.class);

		dag.addVertex(source);
		dag.addVertex(target);
		dag.addEdge(source, target);

		Spliterator<CubeQueryStep> spliterator = TopologicalOrderSpliterator.makeSpliterator(dag);

		Assertions.assertThat(spliterator.trySplit()).isNull();
	}

	@Test
	public void testTopologicalOrderSpliterator_twoParallel() {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag =
				DirectedAcyclicGraph.<CubeQueryStep, DefaultEdge>createBuilder(DefaultEdge.class).build();

		CubeQueryStep source = Mockito.mock(CubeQueryStep.class);
		CubeQueryStep target = Mockito.mock(CubeQueryStep.class);

		dag.addVertex(source);
		dag.addVertex(target);

		Spliterator<CubeQueryStep> spliterator = TopologicalOrderSpliterator.makeSpliterator(dag);

		Assertions.assertThat(spliterator.trySplit()).isNotNull();
	}
}
