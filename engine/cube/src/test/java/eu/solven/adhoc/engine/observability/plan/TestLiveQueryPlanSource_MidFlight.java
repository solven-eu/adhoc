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
package eu.solven.adhoc.engine.observability.plan;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.ListenableFuture;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.query.AdhocQueryId;

/**
 * End-to-end verification that {@code LiveQueryPlanSource.snapshot()} surfaces mid-flight state — i.e. one node in the
 * dag has terminated (cost recorded) while a downstream node is still running. The test pins:
 * <ul>
 * <li>partial-stats correctness: the projector reports the aggregator step as DONE and the combinator step as PENDING
 * while the combinator is genuinely blocked, not "everything PENDING" or "everything DONE";</li>
 * <li>terminal correctness: after releasing the latch and joining the future, all nodes are DONE.</li>
 * </ul>
 *
 * <p>
 * Mechanism: a {@link LatchedCombination} blocks inside {@code combine(...)} until the test releases a shared
 * {@link CountDownLatch}. The test schedules the query asynchronously, awaits the combinator's entry signal (proves the
 * aggregator is already done because the combinator received its input), snapshots the registry, then releases the
 * latch.
 *
 * @author Benoit Lacelle
 */
public class TestLiveQueryPlanSource_MidFlight extends ATestDagInMemory {

	/**
	 * Per-test latch state shared with the combinator (which is instantiated reflectively by the engine and therefore
	 * cannot capture test-instance fields). Set in {@link #feedTable()} so each test method gets fresh latches.
	 */
	static final AtomicReference<LatchedCombination.LatchPair> LATCHES = new AtomicReference<>();

	/**
	 * Registry the test reads to assert mid-flight state. Created here (not in {@code ATestDagRaw}) so we can keep a
	 * direct reference rather than fishing it back out of the cube.
	 */
	BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10_000);

	@Override
	public CubeQueryEngine engine() {
		// Override the default engine() so our registry is wired in. We rebuild from scratch each time the test
		// asks for the engine — cube() is itself memoised, so this only runs once per test.
		return CubeQueryEngine.builder()
				.eventBus(eventBus())
				.factories(makeFactories())
				.queryPlanRegistry(registry)
				.build();
	}

	@BeforeEach
	@Override
	public void feedTable() {
		table().add(Map.of("k1", 100));
		table().add(Map.of("k1", 200));

		LATCHES.set(new LatchedCombination.LatchPair(new CountDownLatch(1), new CountDownLatch(1)));

		forest.addMeasure(Aggregator.sum("k1"));
		forest.addMeasure(Combinator.builder()
				.name("blockedSum")
				.underlying("k1")
				.combinationKey(LatchedCombination.class.getName())
				.build());
	}

	/**
	 * Combinator that blocks inside {@code combine(...)} until the test releases its latch. Instantiated reflectively
	 * by the engine on the executor thread; reads its latch pair from {@link #LATCHES}.
	 */
	public static class LatchedCombination implements ICombination {

		/** Signals "combine() entered" + waits for "the test says release". */
		static final class LatchPair {
			final CountDownLatch entered;
			final CountDownLatch release;

			LatchPair(CountDownLatch entered, CountDownLatch release) {
				this.entered = entered;
				this.release = release;
			}
		}

		@Override
		public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
			LatchPair pair = LATCHES.get();
			if (pair == null) {
				// Defensive: someone ran the engine outside a test that set the latches.
				return underlyingValues.isEmpty() ? null : underlyingValues.get(0);
			}
			pair.entered.countDown();
			try {
				// Bounded wait so a faulty test cannot hang the suite.
				if (!pair.release.await(10, TimeUnit.SECONDS)) {
					throw new IllegalStateException("LatchedCombination waited too long for release");
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("LatchedCombination interrupted", e);
			}
			// Return whatever the underlying produced — the actual value is irrelevant to this test.
			return underlyingValues.isEmpty() ? null : underlyingValues.get(0);
		}
	}

	@Test
	public void testSnapshotShowsMixedDoneAndPendingMidFlight() throws Exception {
		LatchedCombination.LatchPair pair = LATCHES.get();

		// Fire the query asynchronously. The future runs the DAG on the engine's executor; this thread keeps control.
		ListenableFuture<ITabularView> future = cube().executeAsync(CubeQuery.builder().measure("blockedSum").build());

		// Wait until the combinator has entered combine() — at that point the aggregator step has already produced its
		// output (otherwise the combinator would not have its input).
		Assertions.assertThat(pair.entered.await(10, TimeUnit.SECONDS))
				.as("Combinator should reach combine() within 10s")
				.isTrue();

		// The plan source is registered before executeDag runs, so the registry has exactly one entry by now.
		Assertions.assertThat(registry.planCount()).isEqualTo(1);
		AdhocQueryId queryId =
				registry.findIdByUuid(registry.snapshot(anyKnownId()).orElseThrow().getQueryId().getQueryId())
						.orElseThrow();
		QueryPlan midFlight = registry.snapshot(queryId).orElseThrow();

		// Once execution has started (markExecutionStarted was called before executeDag), the plan should report
		// RUNNING — not PENDING, which would imply nothing has begun. PENDING + done-nodes-present produced the
		// "Queued 2/3 steps" UX bug seen in https://imgur.com/… (badge said Queued while half the dag was done).
		Assertions.assertThat(midFlight.getState()).isEqualTo(PlanState.RUNNING);
		Assertions.assertThat(midFlight.getCompletedAt()).isNull();

		// Count node states across the dag. We expect at least one DONE (the aggregator) and at least one PENDING
		// (the combinator, blocked on the latch). The exact tree shape depends on the dag builder, so we count rather
		// than walking by name.
		long doneNodes = countNodes(midFlight.getRoot(), NodeState.DONE);
		long pendingNodes = countNodes(midFlight.getRoot(), NodeState.PENDING);
		Assertions.assertThat(doneNodes)
				.as("At least one DONE node mid-flight (aggregator has finished)")
				.isGreaterThanOrEqualTo(1);
		Assertions.assertThat(pendingNodes)
				.as("At least one PENDING node mid-flight (combinator is blocked)")
				.isGreaterThanOrEqualTo(1);

		// Release the latch and wait for the query to finish.
		pair.release.countDown();
		future.get(10, TimeUnit.SECONDS);

		// Post-completion snapshot: everything should be DONE.
		QueryPlan terminal = registry.snapshot(queryId).orElseThrow();
		Assertions.assertThat(terminal.getState()).isEqualTo(PlanState.DONE);
		Assertions.assertThat(terminal.getCompletedAt()).isNotNull();
		Assertions.assertThat(countNodes(terminal.getRoot(), NodeState.PENDING))
				.as("No PENDING node should remain once the query has completed")
				.isZero();
	}

	@Test
	public void testVersionBumpsOnCompletion() throws Exception {
		LatchedCombination.LatchPair pair = LATCHES.get();

		ListenableFuture<ITabularView> future = cube().executeAsync(CubeQuery.builder().measure("blockedSum").build());
		Assertions.assertThat(pair.entered.await(10, TimeUnit.SECONDS)).isTrue();

		AdhocQueryId queryId = anyKnownId();
		// {@link BoundedQueryPlanRegistry#lookup} is package-private; we use it directly to reach the IPlanSource and
		// read its version. The controllers only need the projected QueryPlan, but this test pins the version-bump
		// contract that pollers rely on for short-circuiting.
		IPlanSource source = registry.lookup(queryId);
		Assertions.assertThat(source).as("source must be present mid-flight").isNotNull();
		long versionMidFlight = source.version();

		// Node count is stable across mid-flight → terminal — the projector walks the full dag both times.
		long nodeCountMid = registry.snapshot(queryId).orElseThrow().getNodeCount();

		pair.release.countDown();
		future.get(10, TimeUnit.SECONDS);

		QueryPlan terminal = registry.snapshot(queryId).orElseThrow();
		Assertions.assertThat(terminal.getCompletedAt()).isNotNull();
		Assertions.assertThat(terminal.getNodeCount()).isEqualTo(nodeCountMid);

		// markCompleted(...) bumps the version — pollers should see at least one increment between the mid-flight
		// snapshot and the post-completion snapshot.
		Assertions.assertThat(source.version())
				.as("version must increment after markCompleted")
				.isGreaterThan(versionMidFlight);
	}

	/**
	 * Resolve the single queryId currently in the registry — there should be exactly one in this test. Used by tests
	 * that don't know the engine-generated UUID upfront.
	 */
	private AdhocQueryId anyKnownId() {
		Assertions.assertThat(registry.sources.size() + registry.locked.size())
				.as("Registry should hold exactly one plan during the test")
				.isEqualTo(1);
		return registry.sources.keySet()
				.stream()
				.findFirst()
				.orElseGet(() -> registry.locked.keySet().iterator().next());
	}

	private static long countNodes(QueryPlanNode root, NodeState state) {
		java.util.ArrayDeque<QueryPlanNode> stack = new java.util.ArrayDeque<>();
		java.util.LinkedHashSet<Object> visited = new java.util.LinkedHashSet<>();
		stack.push(root);
		long count = 0;
		while (!stack.isEmpty()) {
			QueryPlanNode node = stack.pop();
			if (!visited.add(node.getSubject())) {
				continue;
			}
			if (node.getState() == state) {
				count++;
			}
			for (QueryPlanNode child : node.getChildren()) {
				stack.push(child);
			}
		}
		return count;
	}
}
