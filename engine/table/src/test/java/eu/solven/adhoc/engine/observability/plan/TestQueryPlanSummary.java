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

import java.time.Instant;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.AdhocQueryId;

/**
 * Tests {@link QueryPlanSummary#of}. The summarizer counts node states + sums rowsOut + tracks the most-recently
 * completed node — the building blocks of a "this query has been running 2.4s, 12/40 steps done, last finished
 * combinator k1" status line.
 *
 * <p>
 * Hand-builds {@link QueryPlan}s in graph form (flat nodes list + edges list) — same shape the projector produces.
 */
public class TestQueryPlanSummary {

	private static AdhocQueryId newId() {
		return AdhocQueryId.builder().cube("test-cube").build();
	}

	/**
	 * Convenience: build a node with id pre-set. The projector assigns ids; tests can pre-assign them since the
	 * summarizer doesn't depend on id format, only on per-node state / stats.
	 */
	private static QueryPlanNode node(String id, String subject, NodeOperator op, NodeState state, NodeStats stats) {
		return QueryPlanNode.builder()
				.id(id)
				.subject(subject)
				.operator(op)
				.label(subject)
				.state(state)
				.stats(stats)
				.build();
	}

	private static QueryPlanNode node(String id, String subject, NodeOperator op, NodeState state) {
		return node(id, subject, op, state, NodeStats.empty());
	}

	@Test
	public void testCountsByStateAndRowsOut() {
		QueryPlanNode leaf1 = node("n1",
				"leaf1",
				NodeOperator.TABLE_QUERY,
				NodeState.DONE,
				NodeStats.builder().rowsOut(100L).completedAt(Instant.parse("2026-05-14T00:00:05Z")).build());
		QueryPlanNode leaf2 = node("n2",
				"leaf2",
				NodeOperator.TABLE_QUERY,
				NodeState.DONE,
				NodeStats.builder().rowsOut(50L).completedAt(Instant.parse("2026-05-14T00:00:07Z")).build());
		QueryPlanNode pending = node("n3", "pending", NodeOperator.CUBE_STEP, NodeState.PENDING);
		QueryPlanNode root = node("n0", "root", NodeOperator.CUBE_STEP, NodeState.PENDING);

		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.state(PlanState.RUNNING)
				.rootId("n0")
				.nodes(List.of(root, leaf1, leaf2, pending))
				.edges(List.of(QueryPlanEdge.builder().parentId("n0").childId("n1").build(),
						QueryPlanEdge.builder().parentId("n0").childId("n2").build(),
						QueryPlanEdge.builder().parentId("n0").childId("n3").build()))
				.nodeCount(4)
				.build();

		QueryPlanSummary summary = QueryPlanSummary.of(plan, Instant.parse("2026-05-14T00:00:10Z"));

		Assertions.assertThat(summary.getState()).isEqualTo(PlanState.RUNNING);
		Assertions.assertThat(summary.getTotalNodes()).isEqualTo(4);
		Assertions.assertThat(summary.getDoneNodes()).isEqualTo(2);
		Assertions.assertThat(summary.getPendingNodes()).isEqualTo(2); // root + the "pending" leaf
		Assertions.assertThat(summary.getRunningNodes()).isZero();
		Assertions.assertThat(summary.getFailedNodes()).isZero();
		Assertions.assertThat(summary.getTotalRowsOut()).isEqualTo(150L);
		Assertions.assertThat(summary.getLatestCompletedLabel()).isEqualTo("leaf2"); // later completedAt wins
		Assertions.assertThat(summary.getElapsedMs()).isEqualTo(10_000L);
	}

	@Test
	public void testCompletedAtIsUsedWhenAvailable() {
		QueryPlanNode root = node("n0", "root", NodeOperator.CUBE_STEP, NodeState.DONE);
		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.completedAt(Instant.parse("2026-05-14T00:00:03Z"))
				.state(PlanState.DONE)
				.rootId("n0")
				.nodes(List.of(root))
				.nodeCount(1)
				.build();

		// `now` should be ignored for completed plans — elapsedMs uses completedAt − submittedAt.
		QueryPlanSummary summary = QueryPlanSummary.of(plan, Instant.parse("2026-05-14T00:01:00Z"));
		Assertions.assertThat(summary.getElapsedMs()).isEqualTo(3_000L);
	}

	@Test
	public void testStartDelayWhenQueued() {
		// executionStartedAt == null → still queued; startDelayMs grows with `now`.
		QueryPlanNode root = node("n0", "root", NodeOperator.CUBE_STEP, NodeState.PENDING);
		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.state(PlanState.PENDING)
				.rootId("n0")
				.nodes(List.of(root))
				.nodeCount(1)
				.build();

		QueryPlanSummary summary = QueryPlanSummary.of(plan, Instant.parse("2026-05-14T00:00:03Z"));
		Assertions.assertThat(summary.getStartDelayMs()).isEqualTo(3_000L);
	}

	@Test
	public void testStartDelayWhenStarted() {
		// executionStartedAt set → frozen value, independent of `now`.
		QueryPlanNode root = node("n0", "root", NodeOperator.CUBE_STEP, NodeState.RUNNING);
		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.executionStartedAt(Instant.parse("2026-05-14T00:00:02Z"))
				.state(PlanState.RUNNING)
				.rootId("n0")
				.nodes(List.of(root))
				.nodeCount(1)
				.build();

		QueryPlanSummary summary = QueryPlanSummary.of(plan, Instant.parse("2026-05-14T00:01:00Z"));
		Assertions.assertThat(summary.getStartDelayMs()).isEqualTo(2_000L);
	}

	@Test
	public void testSharedChildNotCountedTwice() {
		// DAG-style: a leaf seen from two parents only appears once in `nodes` (the projector dedup) — and the
		// summarizer iterates `nodes` directly, so the count naturally matches.
		QueryPlanNode leaf =
				node("n3", "leaf", NodeOperator.TABLE_QUERY, NodeState.DONE, NodeStats.builder().rowsOut(20L).build());
		QueryPlanNode parentA = node("n1", "parentA", NodeOperator.CUBE_STEP, NodeState.DONE);
		QueryPlanNode parentB = node("n2", "parentB", NodeOperator.CUBE_STEP, NodeState.DONE);
		QueryPlanNode root = node("n0", "root", NodeOperator.CUBE_STEP, NodeState.DONE);

		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.state(PlanState.DONE)
				.rootId("n0")
				.nodes(List.of(root, parentA, parentB, leaf))
				.edges(List.of(QueryPlanEdge.builder().parentId("n0").childId("n1").build(),
						QueryPlanEdge.builder().parentId("n0").childId("n2").build(),
						QueryPlanEdge.builder().parentId("n1").childId("n3").build(),
						QueryPlanEdge.builder().parentId("n2").childId("n3").build()))
				.nodeCount(4)
				.build();

		QueryPlanSummary summary = QueryPlanSummary.of(plan, Instant.parse("2026-05-14T00:00:10Z"));

		// Root + parentA + parentB + leaf — 4 distinct nodes, leaf counted once despite two incoming edges.
		Assertions.assertThat(summary.getTotalNodes()).isEqualTo(4);
		Assertions.assertThat(summary.getTotalRowsOut()).isEqualTo(20L);
	}
}
