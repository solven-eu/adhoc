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
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
		ConcurrentHashMap<ICubeQueryStep, SizeAndDuration> costs = new ConcurrentHashMap<>();
		LiveQueryPlanSource source = buildSource(root, costs);

		// Initial snapshot: no costs → the materialized step (CUBE_QUERY wrapper's only outgoing edge) is PENDING.
		Assertions.assertThat(stepNodeOf(source.snapshot()).getState()).isEqualTo(NodeState.PENDING);

		// Engine writes to the map → next snapshot sees DONE with the recorded stats on the step.
		costs.put(root, SizeAndDuration.builder().size(10L).duration(Duration.ofMillis(5)).build());
		Assertions.assertThat(stepNodeOf(source.snapshot()).getState()).isEqualTo(NodeState.DONE);
		Assertions.assertThat(stepNodeOf(source.snapshot()).getStats().getRowsOut()).isEqualTo(10L);
	}

	/** First child of the CUBE_QUERY wrapper — the cube-step root. Resolves through the plan's edge list. */
	private static QueryPlanNode stepNodeOf(QueryPlan plan) {
		java.util.Map<String, QueryPlanNode> byId =
				plan.getNodes().stream().collect(java.util.stream.Collectors.toMap(QueryPlanNode::getId, n -> n));
		String childId = plan.getEdges()
				.stream()
				.filter(e -> e.getParentId().equals(plan.getRootId()))
				.findFirst()
				.orElseThrow()
				.getChildId();
		return byId.get(childId);
	}

	/**
	 * Children of the cube-step (the CUBE_QUERY wrapper's only child) — typically a single fragment graft. Used to
	 * assert that {@code publishFragment} → {@code snapshot} round-trip wires the graft as an edge from the step.
	 */
	private static java.util.List<QueryPlanNode> stepChildrenOf(QueryPlan plan) {
		QueryPlanNode step = stepNodeOf(plan);
		java.util.Map<String, QueryPlanNode> byId =
				plan.getNodes().stream().collect(java.util.stream.Collectors.toMap(QueryPlanNode::getId, n -> n));
		return plan.getEdges()
				.stream()
				.filter(e -> e.getParentId().equals(step.getId()))
				.map(e -> byId.get(e.getChildId()))
				.toList();
	}

	@Test
	public void testSnapshotReturnsFreshTreePerCall() {
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		QueryPlan a = source.snapshot();
		QueryPlan b = source.snapshot();
		Assertions.assertThat(a).isEqualTo(b);
		Assertions.assertThat(a).isNotSameAs(b);
		// Each snapshot returns a fresh nodes list — equal by value, different instances.
		Assertions.assertThat(a.getNodes()).isNotSameAs(b.getNodes());
	}

	@Test
	public void testVersionStartsAtZeroAndBumps() {
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		Assertions.assertThat(source.version()).isZero();
		Assertions.assertThat(source.bumpVersion()).isEqualTo(1L);
		Assertions.assertThat(source.bumpVersion()).isEqualTo(2L);
		Assertions.assertThat(source.version()).isEqualTo(2L);
	}

	@Test
	public void testIsCompletedTracksPlanState() {
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
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
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		long v0 = source.version();
		source.markCompleted(PlanState.DONE, Instant.now());
		Assertions.assertThat(source.version()).isGreaterThan(v0);
	}

	@Test
	public void testMarkExecutionStartedIsIdempotentAndFlowsToSnapshot() {
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
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
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		registry.registerSource(source);

		QueryPlan viaRegistry = registry.snapshot(source.getQueryId()).orElseThrow();
		Assertions.assertThat(viaRegistry.getQueryId()).isEqualTo(source.getQueryId());
		Assertions.assertThat(viaRegistry.getState()).isEqualTo(PlanState.PENDING);
	}

	@Test
	public void testPublishFragmentAppearsInNextSnapshot() {
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		QueryPlanNode v4 =
				QueryPlanNode.builder().subject("v4-subject").operator(NodeOperator.TABLE_QUERY).label("v4").build();
		long v0 = source.version();
		source.publishFragment(root, v4);

		QueryPlan plan = source.snapshot();
		// The fragment hangs off the materialized step, which is the CUBE_QUERY wrapper's single child.
		Assertions.assertThat(stepChildrenOf(plan))
				.extracting(QueryPlanNode::getSubject)
				.containsExactly(v4.getSubject());
		Assertions.assertThat(source.version()).isGreaterThan(v0);
	}

	@Test
	public void testPublishFragmentDedupesOnSubjectEqualityWithinSameAnchor() {
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		QueryPlanNode v4a =
				QueryPlanNode.builder().subject("shared-subject").operator(NodeOperator.TABLE_QUERY).label("a").build();
		QueryPlanNode v4b = QueryPlanNode.builder()
				.subject("shared-subject")
				.operator(NodeOperator.TABLE_QUERY)
				.label("b (replaces a)")
				.build();
		source.publishFragment(root, v4a);
		source.publishFragment(root, v4b);

		QueryPlan plan = source.snapshot();
		// Only the second fragment survives — the first was replaced because its subject collided. Fragments hang
		// off the materialized step (the CUBE_QUERY wrapper's single child). Match by label since both fragments
		// share the same subject.
		Assertions.assertThat(stepChildrenOf(plan))
				.extracting(QueryPlanNode::getLabel)
				.containsExactly("b (replaces a)");
	}

	@Test
	public void testPublishFragmentRoutedViaRegistry() {
		CubeQueryStep root = CubeQueryStep.builder().measure("mRoot").build();
		LiveQueryPlanSource source = buildSource(root, new ConcurrentHashMap<>());

		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		registry.registerSource(source);

		QueryPlanNode v4 = QueryPlanNode.builder()
				.subject("v4-via-registry")
				.operator(NodeOperator.TABLE_QUERY)
				.label("v4")
				.build();
		registry.publishFragment(source.getQueryId(), root, v4);

		QueryPlan plan = registry.snapshot(source.getQueryId()).orElseThrow();
		// Fragment hangs off the materialized step (the CUBE_QUERY wrapper's single child).
		Assertions.assertThat(stepChildrenOf(plan))
				.extracting(QueryPlanNode::getSubject)
				.containsExactly(v4.getSubject());
	}

	@Test
	public void testPublishFragmentForUnknownQueryIdIsDropped() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId unknown = AdhocQueryId.builder().cube("not-registered").build();
		QueryPlanNode v4 = QueryPlanNode.builder().subject("v4").operator(NodeOperator.TABLE_QUERY).label("v4").build();

		// Must not throw — the production path expects the engine to drop fragments arriving after eviction or
		// for unknown ids (e.g. a wrapper publishing during a unit test that never registered a source).
		registry.publishFragment(unknown, new Object(), v4);
		Assertions.assertThat(registry.snapshot(unknown)).isEmpty();
	}
}
