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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.engine.dag.AdhocDag;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestDagCompletableExecutor {

	private void execute(IAdhocDag<String> dag,
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
		IAdhocDag<String> dag = new AdhocDag<>();

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
		IAdhocDag<String> dag = new AdhocDag<>();

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

	/**
	 * Builds a binary tree DAG with given depth. Each non-leaf node depends on its two children, forming a proper
	 * binary tree where leaves have no outgoing edges.
	 *
	 * @param dag
	 *            the DAG to populate
	 * @param depth
	 *            the tree depth (depth 16 → 2^17 - 1 = 131,071 nodes)
	 * @return the root vertex of the constructed DAG
	 */
	private static String buildBinaryTreeDag(IAdhocDag<String> dag, int depth) {
		dag.addVertex("0");
		for (int d = 0; d < depth; d++) {
			int start = (1 << d) - 1;
			int end = (1 << (d + 1)) - 1;
			for (int i = start; i < end; i++) {
				String parent = String.valueOf(i);
				String left = String.valueOf(2 * i + 1);
				String right = String.valueOf(2 * i + 2);
				dag.addVertex(left);
				dag.addVertex(right);
				dag.addEdge(parent, left);
				dag.addEdge(parent, right);
			}
		}
		return "0";
	}

	/**
	 * Builds a binary tree DAG with ~100K nodes (depth 16 → 2^17 - 1 = 131,071 nodes).
	 *
	 * @return the root vertex of the constructed DAG
	 */
	private static String buildBinaryTreeDag(IAdhocDag<String> dag) {
		return buildBinaryTreeDag(dag, depth);
	}

	/**
	 * Monte Carlo PI estimation. Each task samples random points and counts how many fall inside the unit circle.
	 *
	 * @param samplesPerTask
	 *            number of random points to sample
	 * @param seed
	 *            unique seed per task to ensure deterministic results
	 * @return estimated PI value from this task's samples
	 */
	private static double estimatePi(int samplesPerTask, long seed) {
		Random random = new Random(seed);
		long insideCircle = 0;
		for (int i = 0; i < samplesPerTask; i++) {
			double x = random.nextDouble();
			double y = random.nextDouble();
			if (x * x + y * y <= 1.0) {
				insideCircle++;
			}
		}
		return 4.0 * insideCircle / samplesPerTask;
	}

	/**
	 * Executes the DAG with the given executor, computing PI estimates for each node.
	 *
	 * @param dag
	 *            the DAG to execute
	 * @param root
	 *            the root node to start from
	 * @param executor
	 *            the executor to use
	 * @param results
	 *            map to store results (node → PI estimate)
	 */
	private void executeWithExecutor(IAdhocDag<String> dag,
			String root,
			Executor executor,
			Map<String, Double> results) {
		Set<String> queryStepsDone = ConcurrentHashMap.newKeySet();

		DagCompletableExecutor<String> executor2 = DagCompletableExecutor.<String>builder()
				.fromQueriedToDependencies(dag)
				.queryStepsDone(queryStepsDone)
				.onReadyStep(step -> {
					long stepIndex = Long.parseLong(step);
					int samplesPerTask = 100;
					double piEstimate = estimatePi(samplesPerTask, stepIndex);
					if (sleep > 0) {
						try {
							TimeUnit.MILLISECONDS.sleep(sleep);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							throw new IllegalStateException(e);
						}
					}
					results.put(step, piEstimate);
				})
				.executor(executor)
				.build();

		executor2.executeRecursively(root).join();

		log.info("Parallelism: {}", executor2.tracker.getTimeWeightedParallelism());
	}

	private static final int depth = 15;
	// A sleep is in favor of VirtualThreads as these can all sleep in parallel, as the pool is unbounded.
	private static final int sleep = 0;

	/**
	 * Tests execution of ~130K tasks in a binary tree DAG using VirtualThreadPool. Each node performs a Monte Carlo PI
	 * estimation. This test verifies that VirtualThreadPool can handle a deep DAG with many tasks efficiently.
	 */
	@Test
	public void testExecuteRecursively_100KTasks_VirtualThreadPool() {
		IAdhocDag<String> dag = new AdhocDag<>();
		String root = buildBinaryTreeDag(dag);

		int expectedNodes = (1 << (depth + 1)) - 1;
		Assertions.assertThat(dag.vertexSet()).hasSize(expectedNodes);

		Map<String, Double> results = new ConcurrentHashMap<>();

		try (ExecutorService virtualThreadPool = Executors.newVirtualThreadPerTaskExecutor()) {
			long start = System.currentTimeMillis();
			executeWithExecutor(dag, root, virtualThreadPool, results);
			long duration = System.currentTimeMillis() - start;
			log.info("VirtualThreadPool: {} tasks completed in {} ms", results.size(), duration);
		}

		Assertions.assertThat(results).hasSize(expectedNodes);
	}

	/**
	 * Tests execution of ~130K tasks in a binary tree DAG using ForkJoinPool. Each node performs a Monte Carlo PI
	 * estimation. This test verifies that ForkJoinPool can handle a deep DAG with many tasks efficiently.
	 */
	@Test
	public void testExecuteRecursively_100KTasks_ForkJoinPool() {
		IAdhocDag<String> dag = new AdhocDag<>();
		String root = buildBinaryTreeDag(dag);

		int expectedNodes = (1 << (depth + 1)) - 1;
		Assertions.assertThat(dag.vertexSet()).hasSize(expectedNodes);

		Map<String, Double> results = new ConcurrentHashMap<>();

		try (ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime().availableProcessors())) {
			long start = System.currentTimeMillis();
			executeWithExecutor(dag, root, fjp, results);
			long duration = System.currentTimeMillis() - start;
			log.info("ForkJoinPool: {} tasks completed in {} ms", results.size(), duration);
		}

		Assertions.assertThat(results).hasSize(expectedNodes);
	}

}
