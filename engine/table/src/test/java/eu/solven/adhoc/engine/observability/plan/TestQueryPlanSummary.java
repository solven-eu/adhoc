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
 */
public class TestQueryPlanSummary {

	private static AdhocQueryId newId() {
		return AdhocQueryId.builder().cube("test-cube").build();
	}

	@Test
	public void testCountsByStateAndRowsOut() {
		QueryPlanNode leaf1 = QueryPlanNode.builder()
				.subject("leaf1")
				.operator(NodeOperator.TABLE_QUERY)
				.label("leaf1")
				.state(NodeState.DONE)
				.stats(NodeStats.builder().rowsOut(100L).completedAt(Instant.parse("2026-05-14T00:00:05Z")).build())
				.build();
		QueryPlanNode leaf2 = QueryPlanNode.builder()
				.subject("leaf2")
				.operator(NodeOperator.TABLE_QUERY)
				.label("leaf2")
				.state(NodeState.DONE)
				.stats(NodeStats.builder().rowsOut(50L).completedAt(Instant.parse("2026-05-14T00:00:07Z")).build())
				.build();
		QueryPlanNode pending = QueryPlanNode.builder()
				.subject("pending")
				.operator(NodeOperator.CUBE_STEP)
				.label("pending-step")
				.state(NodeState.PENDING)
				.build();
		QueryPlanNode root = QueryPlanNode.builder()
				.subject("root")
				.operator(NodeOperator.CUBE_STEP)
				.label("root")
				.state(NodeState.PENDING)
				.children(List.of(leaf1, leaf2, pending))
				.build();
		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.state(PlanState.RUNNING)
				.root(root)
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
		QueryPlanNode root = QueryPlanNode.builder()
				.subject("root")
				.operator(NodeOperator.CUBE_STEP)
				.label("root")
				.state(NodeState.DONE)
				.build();
		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.completedAt(Instant.parse("2026-05-14T00:00:03Z"))
				.state(PlanState.DONE)
				.root(root)
				.nodeCount(1)
				.build();

		// `now` should be ignored for completed plans — elapsedMs uses completedAt − submittedAt.
		QueryPlanSummary summary = QueryPlanSummary.of(plan, Instant.parse("2026-05-14T00:01:00Z"));
		Assertions.assertThat(summary.getElapsedMs()).isEqualTo(3_000L);
	}

	@Test
	public void testStartDelayWhenQueued() {
		// executionStartedAt == null → still queued; startDelayMs grows with `now`.
		QueryPlanNode root = QueryPlanNode.builder()
				.subject("root")
				.operator(NodeOperator.CUBE_STEP)
				.label("root")
				.state(NodeState.PENDING)
				.build();
		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.state(PlanState.PENDING)
				.root(root)
				.nodeCount(1)
				.build();

		QueryPlanSummary summary = QueryPlanSummary.of(plan, Instant.parse("2026-05-14T00:00:03Z"));
		Assertions.assertThat(summary.getStartDelayMs()).isEqualTo(3_000L);
	}

	@Test
	public void testStartDelayWhenStarted() {
		// executionStartedAt set → frozen value, independent of `now`.
		QueryPlanNode root = QueryPlanNode.builder()
				.subject("root")
				.operator(NodeOperator.CUBE_STEP)
				.label("root")
				.state(NodeState.RUNNING)
				.build();
		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.executionStartedAt(Instant.parse("2026-05-14T00:00:02Z"))
				.state(PlanState.RUNNING)
				.root(root)
				.nodeCount(1)
				.build();

		QueryPlanSummary summary = QueryPlanSummary.of(plan, Instant.parse("2026-05-14T00:01:00Z"));
		Assertions.assertThat(summary.getStartDelayMs()).isEqualTo(2_000L);
	}

	@Test
	public void testSharedChildNotCountedTwice() {
		// DAG-style: a leaf seen from two parents must only count once.
		QueryPlanNode leaf = QueryPlanNode.builder()
				.subject("leaf")
				.operator(NodeOperator.TABLE_QUERY)
				.label("leaf")
				.state(NodeState.DONE)
				.stats(NodeStats.builder().rowsOut(20L).build())
				.build();
		QueryPlanNode parentA = QueryPlanNode.builder()
				.subject("parentA")
				.operator(NodeOperator.CUBE_STEP)
				.label("parentA")
				.state(NodeState.DONE)
				.children(List.of(leaf))
				.build();
		QueryPlanNode parentB = QueryPlanNode.builder()
				.subject("parentB")
				.operator(NodeOperator.CUBE_STEP)
				.label("parentB")
				.state(NodeState.DONE)
				.children(List.of(leaf))
				.build();
		QueryPlanNode root = QueryPlanNode.builder()
				.subject("root")
				.operator(NodeOperator.CUBE_STEP)
				.label("root")
				.state(NodeState.DONE)
				.children(List.of(parentA, parentB))
				.build();
		QueryPlan plan = QueryPlan.builder()
				.queryId(newId())
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.state(PlanState.DONE)
				.root(root)
				.nodeCount(4)
				.build();

		QueryPlanSummary summary = QueryPlanSummary.of(plan, Instant.parse("2026-05-14T00:00:10Z"));

		// Root + parentA + parentB + leaf (counted once despite two parents).
		Assertions.assertThat(summary.getTotalNodes()).isEqualTo(4);
		Assertions.assertThat(summary.getTotalRowsOut()).isEqualTo(20L); // leaf's rowsOut, not 40
	}
}
