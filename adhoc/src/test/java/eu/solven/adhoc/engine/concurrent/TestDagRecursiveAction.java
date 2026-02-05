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
package eu.solven.adhoc.engine.concurrent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.AtomicLongMap;

public class TestDagRecursiveAction {

	@Test
	public void testConcurrentQuerySteps_sharedAreReUsed() {
		DirectedAcyclicGraph<String, DefaultEdge> dag =
				new DirectedAcyclicGraph<String, DefaultEdge>(DefaultEdge.class);

		dag.addVertex("a");
		dag.addVertex("c");

		int nbIntermediate = 1024;

		IntStream.range(0, nbIntermediate).forEach(i -> {
			dag.addVertex("intermediate_" + i);
			dag.addEdge("a", "intermediate_" + i);
			dag.addEdge("intermediate_" + i, "c");
		});

		AtomicLongMap<String> taskToCount = AtomicLongMap.create();

		DagRecursiveAction<String> rootTask = DagRecursiveAction.<String>builder()
				.fromQueriedToDependencies(dag)
				.step("a")
				.queryStepToValues(new ConcurrentHashMap<>())
				.onReadyStep(string -> {
					taskToCount.incrementAndGet(string);
					if ("c".equals(string)) {
						try {
							// sleep in order to have multiple steps waiting for this shared tasks: if sharing is
							// broken, we may trigger multiple times the same task.
							Thread.sleep(50);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							throw new IllegalStateException(e);
						}
					}
				})
				.build();

		ForkJoinPool.commonPool().submit(rootTask).join();

		Assertions.assertThat(taskToCount.get("a")).isEqualTo(1);
		Assertions.assertThat(taskToCount.get("c")).isEqualTo(1);

		IntStream.range(0, nbIntermediate).forEach(i -> {
			Assertions.assertThat(taskToCount.get("intermediate_" + i)).isEqualTo(1);
		});
	}

}
