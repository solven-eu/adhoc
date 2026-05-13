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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.dag.GraphHelpers;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ICubeQueryStep;
import eu.solven.adhoc.query.AdhocQueryId;

/**
 * Tests for {@link LiveQueryPlanSource}. Simulates the engine writing to {@code stepToCost} between snapshot calls and
 * asserts the projector picks up the new state. Uses a real {@link IAdhocDag} fixture per the projector-test rationale.
 */
public class TestLiveQueryPlanSource {

	private static AdhocQueryId newId() {
		return AdhocQueryId.builder().cube("test-cube").build();
	}

	private static LiveQueryPlanSource buildSource(CubeQueryStep root,
			java.util.Map<ICubeQueryStep, SizeAndDuration> stepToCost) {
		IAdhocDag<CubeQueryStep> graph = GraphHelpers.makeGraph();
		graph.addVertex(root);
		QueryStepsDag dag = QueryStepsDag.builder()
				.inducedToInducer(graph)
				.multigraph(new DirectedMultigraph<>(DefaultEdge.class))
				.explicit(root)
				.stepToCost(stepToCost)
				.build();
		return LiveQueryPlanSource.builder()
				.dag(dag)
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.build();
	}

	@Test
	public void testSnapshotReflectsCurrentStepToCost() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		ConcurrentHashMap<ICubeQueryStep, SizeAndDuration> costs = new ConcurrentHashMap<>();
		LiveQueryPlanSource source = buildSource(root, costs);

		// Initial snapshot: no costs → PENDING.
		Assertions.assertThat(source.snapshot().getRoot().getState()).isEqualTo(NodeState.PENDING);

		// Engine writes to the map → next snapshot sees DONE with the recorded stats.
		costs.put(root, SizeAndDuration.builder().size(10L).duration(Duration.ofMillis(5)).build());
		Assertions.assertThat(source.snapshot().getRoot().getState()).isEqualTo(NodeState.DONE);
		Assertions.assertThat(source.snapshot().getRoot().getStats().getRowsOut()).isEqualTo(10L);
	}

	@Test
	public void testSnapshotReturnsFreshTreePerCall() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		QueryPlan a = source.snapshot();
		QueryPlan b = source.snapshot();
		Assertions.assertThat(a).isEqualTo(b);
		Assertions.assertThat(a).isNotSameAs(b);
		Assertions.assertThat(a.getRoot()).isNotSameAs(b.getRoot());
	}

	@Test
	public void testVersionStartsAtZeroAndBumps() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		Assertions.assertThat(source.version()).isZero();
		Assertions.assertThat(source.bumpVersion()).isEqualTo(1L);
		Assertions.assertThat(source.bumpVersion()).isEqualTo(2L);
		Assertions.assertThat(source.version()).isEqualTo(2L);
	}

	@Test
	public void testIsCompletedTracksPlanState() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		Assertions.assertThat(source.isCompleted()).isFalse();
		Assertions.assertThat(source.snapshot().getState()).isEqualTo(PlanState.PENDING);

		source.markCompleted(PlanState.DONE, Instant.parse("2026-05-14T00:00:10Z"));
		Assertions.assertThat(source.isCompleted()).isTrue();

		QueryPlan after = source.snapshot();
		Assertions.assertThat(after.getState()).isEqualTo(PlanState.DONE);
		Assertions.assertThat(after.getCompletedAt()).isEqualTo(Instant.parse("2026-05-14T00:00:10Z"));
	}

	@Test
	public void testMarkCompletedBumpsVersion() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		long v0 = source.version();
		source.markCompleted(PlanState.DONE, Instant.now());
		Assertions.assertThat(source.version()).isGreaterThan(v0);
	}

	@Test
	public void testMarkExecutionStartedIsIdempotentAndFlowsToSnapshot() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		Assertions.assertThat(source.snapshot().getExecutionStartedAt()).isNull();

		Instant started = Instant.parse("2026-05-14T00:00:01Z");
		source.markExecutionStarted(started);
		Assertions.assertThat(source.snapshot().getExecutionStartedAt()).isEqualTo(started);

		// Second call must not overwrite (idempotent — first call wins so recorded delay isn't reset).
		source.markExecutionStarted(Instant.parse("2026-05-14T00:01:00Z"));
		Assertions.assertThat(source.snapshot().getExecutionStartedAt()).isEqualTo(started);
	}

	@Test
	public void testLiveSourceCanBeRegisteredInRegistry() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		registry.registerSource(source);

		QueryPlan viaRegistry = registry.snapshot(source.getQueryId()).orElseThrow();
		Assertions.assertThat(viaRegistry.getQueryId()).isEqualTo(source.getQueryId());
		Assertions.assertThat(viaRegistry.getState()).isEqualTo(PlanState.PENDING);
	}
}
