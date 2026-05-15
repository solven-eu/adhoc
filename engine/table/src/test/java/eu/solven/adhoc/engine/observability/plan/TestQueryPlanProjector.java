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
 * Tests for {@link QueryPlanProjector}. Uses real {@link IAdhocDag} fixtures rather than mocks — JGraphT's
 * {@code Graph} surface is too rich to mock methodically (the projector + {@link GraphHelpers#getRoots} reach into
 * {@code inDegreeOf}, {@code getEdgeSource}, etc.). Real graphs are clearer and exercise the same code path the engine
 * does.
 */
public class TestQueryPlanProjector {

	private static AdhocQueryId newId() {
		return AdhocQueryId.builder().cube("test-cube").build();
	}

	/** Build a single-vertex dag wrapped in a QueryStepsDag. */
	private static QueryStepsDag oneRootDag(CubeQueryStep root,
			java.util.Map<ICubeQueryStep, SizeAndDuration> stepToCost) {
		IAdhocDag<CubeQueryStep> graph = GraphHelpers.makeGraph();
		graph.addVertex(root);
		return QueryStepsDag.builder()
				.inducedToInducer(graph)
				.multigraph(new DirectedMultigraph<>(DefaultEdge.class))
				.explicit(root)
				.stepToCost(stepToCost)
				.build();
	}

	@Test
	public void testPendingStepHasEmptyStats() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		Mockito.when(root.toString()).thenReturn("root-step");
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		AdhocQueryId queryId = newId();
		QueryPlan plan = new QueryPlanProjector().project(dag,
				queryId,
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null);

		Assertions.assertThat(plan.getQueryId()).isEqualTo(queryId);
		Assertions.assertThat(plan.getState()).isEqualTo(PlanState.PENDING);
		Assertions.assertThat(plan.getRoot().getSubject()).isSameAs(root);
		Assertions.assertThat(plan.getRoot().getState()).isEqualTo(NodeState.PENDING);
		Assertions.assertThat(plan.getRoot().getStats()).isEqualTo(NodeStats.empty());
		Assertions.assertThat(plan.getRoot().getChildren()).isEmpty();
	}

	@Test
	public void testDoneStepCarriesSizeAndDuration() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		Mockito.when(root.toString()).thenReturn("root-step");
		ConcurrentHashMap<ICubeQueryStep, SizeAndDuration> costs = new ConcurrentHashMap<>();
		costs.put(root, SizeAndDuration.builder().size(42L).duration(Duration.ofMillis(123)).build());
		QueryStepsDag dag = oneRootDag(root, costs);

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.DONE,
				Instant.parse("2026-05-14T00:00:01Z"));

		Assertions.assertThat(plan.getRoot().getState()).isEqualTo(NodeState.DONE);
		Assertions.assertThat(plan.getRoot().getStats().getRowsOut()).isEqualTo(42L);
		Assertions.assertThat(plan.getRoot().getStats().getElapsedMs()).isEqualTo(123L);
	}

	@Test
	public void testParentChildEdgesPropagatedAsChildren() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class, "root");
		CubeQueryStep leaf = Mockito.mock(CubeQueryStep.class, "leaf");
		IAdhocDag<CubeQueryStep> graph = GraphHelpers.makeGraph();
		graph.addVertex(root);
		graph.addVertex(leaf);
		graph.addEdge(root, leaf);

		QueryStepsDag dag = QueryStepsDag.builder()
				.inducedToInducer(graph)
				.multigraph(new DirectedMultigraph<>(DefaultEdge.class))
				.explicit(root)
				.stepToCost(new ConcurrentHashMap<>())
				.build();

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null);

		Assertions.assertThat(plan.getRoot().getSubject()).isSameAs(root);
		Assertions.assertThat(plan.getRoot().getChildren()).hasSize(1);
		Assertions.assertThat(plan.getRoot().getChildren().get(0).getSubject()).isSameAs(leaf);
		Assertions.assertThat(plan.getNodeCount()).isEqualTo(2);
	}

	@Test
	public void testCustomMarkerPropagatedToPlan() {
		// The projector reads the plan-level customMarker from the dag's first explicit step, not from a parameter —
		// the marker is per-step (and may vary across the dag), so the dag is the source of truth.
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		Mockito.when(root.getCustomMarker()).thenReturn("JPY");
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null);

		Assertions.assertThat(plan.getSubmittedCustomMarker()).isEqualTo("JPY");
	}

	@Test
	public void testParentQueryIdPropagatedToPlan() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		java.util.UUID parentUuid = java.util.UUID.randomUUID();
		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				parentUuid,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null);

		Assertions.assertThat(plan.getParentQueryId()).isEqualTo(parentUuid);
	}

	@Test
	public void testSharedChildNotDuplicated() {
		// Both roots fan-out to the same leaf. The projector memoizes so the leaf becomes ONE QueryPlanNode that
		// both parents point at (DAG property; otherwise a deep merge graph would explode the node count).
		CubeQueryStep rootA = Mockito.mock(CubeQueryStep.class, "rootA");
		CubeQueryStep rootB = Mockito.mock(CubeQueryStep.class, "rootB");
		CubeQueryStep leaf = Mockito.mock(CubeQueryStep.class, "leaf");
		IAdhocDag<CubeQueryStep> graph = GraphHelpers.makeGraph();
		graph.addVertex(rootA);
		graph.addVertex(rootB);
		graph.addVertex(leaf);
		graph.addEdge(rootA, leaf);
		graph.addEdge(rootB, leaf);

		QueryStepsDag dag = QueryStepsDag.builder()
				.inducedToInducer(graph)
				.multigraph(new DirectedMultigraph<>(DefaultEdge.class))
				.explicit(rootA)
				.explicit(rootB)
				.stepToCost(new ConcurrentHashMap<>())
				.build();

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null);

		// Synthetic root wraps the two real roots.
		Assertions.assertThat(plan.getRoot().getChildren()).hasSize(2);
		QueryPlanNode rootAnode = plan.getRoot().getChildren().get(0);
		QueryPlanNode rootBnode = plan.getRoot().getChildren().get(1);
		// Each real root has the same single leaf child — the SAME QueryPlanNode instance (memoized).
		Assertions.assertThat(rootAnode.getChildren().get(0)).isSameAs(rootBnode.getChildren().get(0));
		// Node count: leaf counted once + 2 roots + synthetic root = 4.
		Assertions.assertThat(plan.getNodeCount()).isEqualTo(4);
	}

	// --- Fragment grafting -------------------------------------------------------------------

	/**
	 * A fragment published under a node's {@code subject} appears as an additional child in the next snapshot. The
	 * original cube DAG children stay present — fragments add, never replace.
	 */
	@Test
	public void testProject_singleFragmentGraftsAsAdditionalChild() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		Mockito.when(root.toString()).thenReturn("root-step");
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		// Fragment with a custom subject mimicking a `TableQueryV4` reference.
		Object v4Subject = new Object();
		QueryPlanNode v4Node =
				QueryPlanNode.builder().subject(v4Subject).operator(NodeOperator.TABLE_QUERY).label("v4").build();
		java.util.Map<Object, java.util.List<QueryPlanNode>> fragments =
				java.util.Map.of(root, java.util.List.of(v4Node));

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null,
				fragments);

		Assertions.assertThat(plan.getRoot().getChildren()).containsExactly(v4Node);
		// Node count: root + 1 fragment = 2.
		Assertions.assertThat(plan.getNodeCount()).isEqualTo(2);
	}

	/**
	 * Two fragments anchored on the same node stack as siblings. Subject-based dedup is by individual fragment subject
	 * — different subjects ARE different fragments and must not collapse.
	 */
	@Test
	public void testProject_multipleFragmentsUnderSameAnchorAreSiblings() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		Mockito.when(root.toString()).thenReturn("root-step");
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		QueryPlanNode v4a =
				QueryPlanNode.builder().subject("v4-a").operator(NodeOperator.TABLE_QUERY).label("a").build();
		QueryPlanNode v4b =
				QueryPlanNode.builder().subject("v4-b").operator(NodeOperator.TABLE_QUERY).label("b").build();
		java.util.Map<Object, java.util.List<QueryPlanNode>> fragments =
				java.util.Map.of(root, java.util.List.of(v4a, v4b));

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null,
				fragments);

		Assertions.assertThat(plan.getRoot().getChildren()).containsExactly(v4a, v4b);
	}

	/**
	 * Deeper anchors resolve recursively: a fragment grafted under the cube root can itself host fragments anchored on
	 * its own subject. This is exactly how the SQL-leaf graft works on top of the table-engine TABLE_QUERY graft.
	 */
	@Test
	public void testProject_deepFragmentChainGraftsRecursively() {
		CubeQueryStep root = Mockito.mock(CubeQueryStep.class);
		Mockito.when(root.toString()).thenReturn("root-step");
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		Object v4Subject = new Object();
		QueryPlanNode v4Node =
				QueryPlanNode.builder().subject(v4Subject).operator(NodeOperator.TABLE_QUERY).label("v4").build();
		QueryPlanNode sqlLeaf = QueryPlanNode.builder()
				.subject("sql-leaf-subject")
				.operator(NodeOperator.TABLE_QUERY)
				.label("sql")
				.details(java.util.Map.of("language", "sql", "sql", "select 1"))
				.build();
		java.util.Map<Object, java.util.List<QueryPlanNode>> fragments =
				java.util.Map.of(root, java.util.List.of(v4Node), v4Subject, java.util.List.of(sqlLeaf));

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null,
				fragments);

		// Tree: root → v4Node → sqlLeaf. The v4Node was a fragment; its child slot is filled lazily via the second
		// fragment anchored on its subject.
		QueryPlanNode v4Projected = plan.getRoot().getChildren().get(0);
		Assertions.assertThat(v4Projected.getSubject()).isSameAs(v4Subject);
		Assertions.assertThat(v4Projected.getChildren()).hasSize(1);
		Assertions.assertThat(v4Projected.getChildren().get(0).getDetails()).containsEntry("language", "sql");
		// Node count: root + v4 + sql = 3.
		Assertions.assertThat(plan.getNodeCount()).isEqualTo(3);
	}
}
