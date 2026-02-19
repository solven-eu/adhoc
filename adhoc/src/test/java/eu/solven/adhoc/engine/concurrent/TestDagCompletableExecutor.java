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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.AtomicLongMap;

public class TestDagCompletableExecutor {

	private void execute(DirectedAcyclicGraph<String, DefaultEdge> dag,
			AtomicLongMap<String> taskToCount,
			Map<String, Object> queryStepToValues,
			AtomicInteger nbExecution) {
		DagCompletableExecutor<String> rootTask = DagCompletableExecutor.<String>builder()
				.fromQueriedToDependencies(dag)
				// .step("a")
				.queryStepsDone(queryStepToValues.keySet())
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

					queryStepToValues.put(string, nbExecution.getAndIncrement());
				})
				.executor(ForkJoinPool.commonPool())
				.build();

		rootTask.executeRecursively("a").join();
	}

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
		Map<String, Object> queryStepToValues = new ConcurrentHashMap<>();
		AtomicInteger nbExecution = new AtomicInteger();

		execute(dag, taskToCount, queryStepToValues, nbExecution);

		Assertions.assertThat(taskToCount.get("a")).isEqualTo(1);
		Assertions.assertThat(taskToCount.get("c")).isEqualTo(1);

		IntStream.range(0, nbIntermediate).forEach(i -> {
			Assertions.assertThat(taskToCount.get("intermediate_" + i)).isEqualTo(1);
		});

		// initial + layer1 + final
		Assertions.assertThat(nbExecution).hasValue(1 + nbIntermediate + 1);
	}

	// This test tries to demonstrate that even with a complex DAG, we do not end with thread starving
	// In fact, we sometimes observe StackOverFlow, and rarely thread-starving in this unit-test
	// Anyway, it suggests FJP is too fragile for complex DAG
	// @Disabled("Demonstrate fragility with complex DAG in FJP")
	@Test
	public void testConcurrentQuerySteps_sharedAreReUsed_2layers() {
		DirectedAcyclicGraph<String, DefaultEdge> dag =
				new DirectedAcyclicGraph<String, DefaultEdge>(DefaultEdge.class);

		dag.addVertex("a");
		dag.addVertex("c");

		int nbIntermediate = 256;

		// a to layer1
		IntStream.range(0, nbIntermediate).forEach(i -> {
			dag.addVertex("intermediate1_" + i);
			dag.addEdge("a", "intermediate1_" + i);
		});

		// layer1 to layer2
		IntStream.range(0, nbIntermediate).forEach(i1 -> {
			dag.addVertex("intermediate2_" + i1);
		});
		IntStream.range(0, nbIntermediate).forEach(i1 -> {
			IntStream.range(0, nbIntermediate).forEach(i2 -> {
				dag.addEdge("intermediate1_" + i1, "intermediate2_" + i2);
			});
		});

		// layer2 to c
		IntStream.range(0, nbIntermediate).forEach(i -> {
			dag.addEdge("intermediate2_" + i, "c");
		});

		AtomicLongMap<String> taskToCount = AtomicLongMap.create();

		AtomicInteger nbExecution = new AtomicInteger();
		Map<String, Object> queryStepToValues = new ConcurrentHashMap<>();

		execute(dag, taskToCount, queryStepToValues, nbExecution);

		Assertions.assertThat(taskToCount.get("a")).isEqualTo(1);
		Assertions.assertThat(taskToCount.get("c")).isEqualTo(1);

		IntStream.range(0, nbIntermediate).forEach(i -> {
			Assertions.assertThat(taskToCount.get("intermediate1_" + i)).isEqualTo(1);
			Assertions.assertThat(taskToCount.get("intermediate2_" + i)).isEqualTo(1);
		});

		// initial + layer1 + layer2 + final
		Assertions.assertThat(nbExecution).hasValue(1 + nbIntermediate + nbIntermediate + 1);
	}

}
