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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.model.measure.ReferencedMeasure;
import eu.solven.adhoc.model.query.IGroupBy;
import eu.solven.adhoc.query.AdhocQueryId;

/**
 * Tests for {@link QueryPlanProjector}. Uses real {@link IAdhocDag} fixtures rather than mocks — JGraphT's
 * {@code Graph} surface is too rich to mock methodically (the projector + {@link GraphHelpers#getRoots} reach into
 * {@code inDegreeOf}, {@code getEdgeSource}, etc.). Real graphs are clearer and exercise the same code path the engine
 * does.
 *
 * <p>
 * The plan model is now graph-shaped ({@code rootId} + flat {@code nodes} / {@code edges} lists), so these tests walk
 * connectivity via {@link #childrenOf} rather than {@code plan.getRoot().getChildren()}.
 */
public class TestQueryPlanProjector {

	private static AdhocQueryId newId() {
		return AdhocQueryId.builder().cube("test-cube").build();
	}

	/** Build a single-vertex dag wrapped in a QueryStepsDag. */
	private static QueryStepsDag oneRootDag(CubeQueryStep root, Map<ICubeQueryStep, SizeAndDuration> stepToCost) {
		IAdhocDag<CubeQueryStep> graph = GraphHelpers.makeGraph();
		graph.addVertex(root);
		return QueryStepsDag.builder()
				.inducedToInducer(graph)
				.multigraph(new DirectedMultigraph<>(DefaultEdge.class))
				.explicit(root)
				.stepToCost(stepToCost)
				.build();
	}

	/**
	 * Resolve the {@link QueryPlanNode} carrying the given id. Throws if absent (test fixtures should always find one).
	 */
	private static QueryPlanNode nodeById(QueryPlan plan, String id) {
		return plan.getNodes().stream().filter(n -> n.getId().equals(id)).findFirst().orElseThrow();
	}

	/** Children of {@code parentId} resolved through {@link QueryPlan#getEdges()}. Preserves edge-emission order. */
	private static List<QueryPlanNode> childrenOf(QueryPlan plan, String parentId) {
		Map<String, QueryPlanNode> byId =
				plan.getNodes().stream().collect(Collectors.toMap(QueryPlanNode::getId, n -> n));
		return plan.getEdges()
				.stream()
				.filter(e -> e.getParentId().equals(parentId))
				.map(e -> byId.get(e.getChildId()))
				.toList();
	}

	private static QueryPlanNode rootNode(QueryPlan plan) {
		return nodeById(plan, plan.getRootId());
	}

	@Test
	public void testToDetails() {
		CubeQueryStep step = CubeQueryStep.builder()
				.measure(ReferencedMeasure.ref("a"))
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(IGroupBy.GRAND_TOTAL)
				.customMarker("someCustomMarker")
				.build();

		Assertions.assertThat(QueryPlanProjector.toDetails(step))
				.hasSize(2)
				.containsEntry("measure", "ReferencedMeasure(ref=a)")
				.containsEntry("customMarker", "someCustomMarker");
	}

	@Test
	public void testPendingStepHasEmptyStats() {
		CubeQueryStep root = CubeQueryStep.builder().measure("m").build();
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
		// The root is always a CUBE_QUERY wrapper (single source of truth for the registry's unit of work). The
		// materialized cube-step sits one level deeper, reached via an edge from the wrapper.
		Assertions.assertThat(rootNode(plan).getOperator()).isEqualTo(NodeOperator.CUBE_QUERY);
		List<QueryPlanNode> rootChildren = childrenOf(plan, plan.getRootId());
		Assertions.assertThat(rootChildren).hasSize(1);
		QueryPlanNode stepNode = rootChildren.get(0);
		Assertions.assertThat(stepNode.getSubject()).isSameAs(root);
		Assertions.assertThat(stepNode.getState()).isEqualTo(NodeState.PENDING);
		Assertions.assertThat(stepNode.getStats()).isEqualTo(NodeStats.empty());
		// Cube-step has no outgoing edges → no children.
		Assertions.assertThat(childrenOf(plan, stepNode.getId())).isEmpty();
	}

	@Test
	public void testDoneStepCarriesSizeAndDuration() {
		CubeQueryStep root = CubeQueryStep.builder().measure("m").build();
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

		QueryPlanNode stepNode = childrenOf(plan, plan.getRootId()).get(0);
		Assertions.assertThat(stepNode.getState()).isEqualTo(NodeState.DONE);
		Assertions.assertThat(stepNode.getStats().getRowsOut()).isEqualTo(42L);
		Assertions.assertThat(stepNode.getStats().getElapsedMs()).isEqualTo(123L);
	}

	@Test
	public void testParentChildEdgesPropagatedAsEdges() {
		CubeQueryStep root = CubeQueryStep.builder().measure("m").build();
		CubeQueryStep leaf = CubeQueryStep.builder().measure("mLeaf").build();
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

		QueryPlanNode stepNode = childrenOf(plan, plan.getRootId()).get(0);
		Assertions.assertThat(stepNode.getSubject()).isSameAs(root);
		List<QueryPlanNode> stepChildren = childrenOf(plan, stepNode.getId());
		Assertions.assertThat(stepChildren).hasSize(1);
		Assertions.assertThat(stepChildren.get(0).getSubject()).isSameAs(leaf);
		// Node count: root + leaf + CUBE_QUERY wrapper = 3.
		Assertions.assertThat(plan.getNodeCount()).isEqualTo(3);
	}

	@Test
	public void testCustomMarkerPropagatedToPlan() {
		// The projector reads the plan-level customMarker from the dag's first explicit step, not from a parameter —
		// the marker is per-step (and may vary across the dag), so the dag is the source of truth.
		CubeQueryStep root = CubeQueryStep.builder().measure("m").customMarker("JPY").build();
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
		CubeQueryStep root = CubeQueryStep.builder().measure("m").build();
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
		// Both roots fan-out to the same leaf. The projector dedupes by subject equality so the leaf becomes ONE
		// {@link QueryPlanNode} that two edges point at — DAG property; otherwise a deep merge graph would explode
		// the node count.
		CubeQueryStep rootA = CubeQueryStep.builder().measure("mRootA").build();
		CubeQueryStep rootB = CubeQueryStep.builder().measure("mRootB").build();
		CubeQueryStep leaf = CubeQueryStep.builder().measure("mLeaf").build();
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

		Assertions.assertThat(rootNode(plan).getOperator()).isEqualTo(NodeOperator.CUBE_QUERY);
		List<QueryPlanNode> rootChildren = childrenOf(plan, plan.getRootId());
		Assertions.assertThat(rootChildren).hasSize(2);
		// Each real root has the same single leaf child — same id, dedup'd by subject.
		QueryPlanNode rootAleaf = childrenOf(plan, rootChildren.get(0).getId()).get(0);
		QueryPlanNode rootBleaf = childrenOf(plan, rootChildren.get(1).getId()).get(0);
		Assertions.assertThat(rootAleaf.getId()).isEqualTo(rootBleaf.getId());
		// Node count: leaf counted once + 2 roots + CUBE_QUERY wrapper = 4.
		Assertions.assertThat(plan.getNodeCount()).isEqualTo(4);
		// The leaf has exactly TWO incoming edges (one per root) — the DAG fan-in.
		Assertions.assertThat(plan.getEdges().stream().filter(e -> e.getChildId().equals(rootAleaf.getId())).count())
				.isEqualTo(2L);
	}

	// --- Fragment grafting -------------------------------------------------------------------

	/**
	 * A fragment published under a node's {@code subject} appears as an additional child in the next snapshot. The
	 * original cube DAG children stay present — fragments add, never replace.
	 */
	@Test
	public void testProject_singleFragmentGraftsAsAdditionalChild() {
		CubeQueryStep root = CubeQueryStep.builder().measure("m").build();
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		// Fragment with a custom subject mimicking a `TableQueryV4` reference.
		Object v4Subject = new Object();
		QueryPlanNode v4Node =
				QueryPlanNode.builder().subject(v4Subject).operator(NodeOperator.TABLE_QUERY).label("v4").build();
		Map<Object, List<QueryPlanNode>> fragments = Map.of(root, List.of(v4Node));

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null,
				fragments);

		QueryPlanNode stepNode = childrenOf(plan, plan.getRootId()).get(0);
		List<QueryPlanNode> stepChildren = childrenOf(plan, stepNode.getId());
		Assertions.assertThat(stepChildren).hasSize(1);
		Assertions.assertThat(stepChildren.get(0).getSubject()).isSameAs(v4Subject);
		// Node count: root + 1 fragment + CUBE_QUERY wrapper = 3.
		Assertions.assertThat(plan.getNodeCount()).isEqualTo(3);
	}

	/**
	 * Two fragments anchored on the same node stack as siblings. Subject-based dedup is by individual fragment subject
	 * — different subjects ARE different fragments and must not collapse.
	 */
	@Test
	public void testProject_multipleFragmentsUnderSameAnchorAreSiblings() {
		CubeQueryStep root = CubeQueryStep.builder().measure("m").build();
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		QueryPlanNode v4a =
				QueryPlanNode.builder().subject("v4-a").operator(NodeOperator.TABLE_QUERY).label("a").build();
		QueryPlanNode v4b =
				QueryPlanNode.builder().subject("v4-b").operator(NodeOperator.TABLE_QUERY).label("b").build();
		Map<Object, List<QueryPlanNode>> fragments = Map.of(root, List.of(v4a, v4b));

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null,
				fragments);

		QueryPlanNode stepNode = childrenOf(plan, plan.getRootId()).get(0);
		List<Object> graftSubjects =
				childrenOf(plan, stepNode.getId()).stream().map(QueryPlanNode::getSubject).toList();
		Assertions.assertThat(graftSubjects).containsExactly("v4-a", "v4-b");
	}

	/**
	 * Deeper anchors resolve recursively: a fragment grafted under the cube root can itself host fragments anchored on
	 * its own subject. This is exactly how the SQL-leaf graft works on top of the table-engine TABLE_QUERY graft.
	 */
	@Test
	public void testProject_deepFragmentChainGraftsRecursively() {
		CubeQueryStep root = CubeQueryStep.builder().measure("m").build();
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		Object v4Subject = new Object();
		QueryPlanNode v4Node =
				QueryPlanNode.builder().subject(v4Subject).operator(NodeOperator.TABLE_QUERY).label("v4").build();
		QueryPlanNode sqlLeaf = QueryPlanNode.builder()
				.subject("sql-leaf-subject")
				.operator(NodeOperator.TABLE_QUERY)
				.label("sql")
				.details(Map.of("language", "sql", "sql", "select 1"))
				.build();
		Map<Object, List<QueryPlanNode>> fragments = Map.of(root, List.of(v4Node), v4Subject, List.of(sqlLeaf));

		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null,
				fragments);

		// Chain: CUBE_QUERY wrapper → cube-step root → v4Node → sqlLeaf. The v4Node was a fragment; its child slot
		// is filled lazily via the second fragment anchored on its subject.
		QueryPlanNode stepNode = childrenOf(plan, plan.getRootId()).get(0);
		QueryPlanNode v4Projected = childrenOf(plan, stepNode.getId()).get(0);
		Assertions.assertThat(v4Projected.getSubject()).isSameAs(v4Subject);
		List<QueryPlanNode> v4Children = childrenOf(plan, v4Projected.getId());
		Assertions.assertThat(v4Children).hasSize(1);
		Assertions.assertThat(v4Children.get(0).getDetails()).containsEntry("language", "sql");
		// Node count: root + v4 + sql + CUBE_QUERY wrapper = 4.
		Assertions.assertThat(plan.getNodeCount()).isEqualTo(4);
	}

	/**
	 * Regression: a fragment whose root subject is its own anchor used to send the projector into an infinite recursion
	 * (graftRecursive → appendFragments → graftRecursive of the same node, repeated forever). The new graph-form
	 * projector handles this naturally — the {@code subjectToId} map dedupes on re-entry, so the self-referential
	 * fragment is allocated once and gets no extra incoming edge from itself.
	 */
	@Test
	public void testProject_fragmentWhoseSubjectIsItsOwnAnchorDoesNotLoop() {
		CubeQueryStep root = CubeQueryStep.builder().measure("m").build();
		QueryStepsDag dag = oneRootDag(root, new ConcurrentHashMap<>());

		// Fragment anchored on `root` AND with subject = root → would loop on the old tree-walking projector.
		QueryPlanNode selfRefFragment = QueryPlanNode.builder()
				.subject(root)
				.operator(NodeOperator.TABLE_STEP)
				.label("induced-loop-trigger")
				.build();
		Map<Object, List<QueryPlanNode>> fragments = Map.of(root, List.of(selfRefFragment));

		// If the dedup regresses, this call hangs forever. The assertions below confirm the projector returned
		// with a finite graph.
		QueryPlan plan = new QueryPlanProjector().project(dag,
				newId(),
				null,
				"test-cube",
				Instant.parse("2026-05-14T00:00:00Z"),
				null,
				PlanState.PENDING,
				null,
				fragments);

		QueryPlanNode stepNode = childrenOf(plan, plan.getRootId()).get(0);
		// The self-referential fragment shares its subject with the cube step, so the projector emits a SINGLE node
		// for both. The cube-step node IS the fragment node — no separate child gets allocated.
		Assertions.assertThat(stepNode.getSubject()).isSameAs(root);
		// And: no edge from the node to itself (would be a true cycle).
		Assertions.assertThat(plan.getEdges()
				.stream()
				.anyMatch(e -> e.getParentId().equals(stepNode.getId()) && e.getChildId().equals(stepNode.getId())))
				.isFalse();
	}
}
