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

import org.jspecify.annotations.Nullable;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Concise, single-line-friendly description of a {@link QueryPlan}'s current state — the answer to "the query has been
 * running for a while; what's happening?". Designed for high-frequency polling (the UI can refresh a status chip at 2
 * Hz cheaper than a full snapshot/render cycle) and for log-friendly textual rendering.
 *
 * <p>
 * Fields are derived by walking the plan tree once and counting; allocation is just this value object, not a fresh
 * tree. {@link IPlanSource} implementations expose {@link IPlanSource#summary()} as a cheap shortcut so the live source
 * can produce a summary without materializing the immutable plan tree (the dag-walk produces both at the same
 * per-snapshot cost — but pollers may only need the summary).
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
public class QueryPlanSummary {
	/** Aggregate plan state. Mirrors {@link QueryPlan#getState()}. */
	@NonNull
	PlanState state;

	/** Total nodes in the plan. */
	long totalNodes;

	/** Nodes whose state is {@link NodeState#DONE}. */
	long doneNodes;

	/**
	 * Nodes whose state is {@link NodeState#PENDING}. Today's projector reports PENDING for both not-yet-started AND
	 * in-progress; a follow-up engine change will introduce a true RUNNING bucket.
	 */
	long pendingNodes;

	/** Nodes whose state is {@link NodeState#RUNNING}. Will populate once the engine exposes a per-step running set. */
	long runningNodes;

	/** Nodes whose state is {@link NodeState#FAILED}. */
	long failedNodes;

	/**
	 * Wall-clock since {@link QueryPlan#getSubmittedAt()}; for completed plans this is
	 * {@code completedAt − submittedAt}.
	 */
	long elapsedMs;

	/**
	 * Time spent waiting between {@link QueryPlan#getSubmittedAt() submission} and
	 * {@link QueryPlan#getExecutionStartedAt() execution start}. Typically a few millis on an idle system, but can grow
	 * to seconds (or minutes) when the query pool is saturated and the new query is queued behind others.
	 *
	 * <p>
	 * Semantics:
	 * <ul>
	 * <li>Execution has started ({@code executionStartedAt != null}): {@code executionStartedAt − submittedAt}.</li>
	 * <li>Still queued ({@code executionStartedAt == null}): {@code now − submittedAt} — the "delay so far". The value
	 * keeps growing across polls until execution begins.</li>
	 * </ul>
	 */
	long startDelayMs;

	/** Sum of {@link NodeStats#getRowsOut()} across every DONE node. {@code 0} when no stats are populated yet. */
	long totalRowsOut;

	/**
	 * Label of the most-recently completed node (max {@code completedAt}). {@code null} when no node has finished yet.
	 * Useful for "Last finished: combinator k1.cube — 12 ms" status lines.
	 */
	@Nullable
	String latestCompletedLabel;

	/**
	 * Build a summary from a fully-projected {@link QueryPlan}. Single pass over the tree.
	 *
	 * @param plan
	 *            a plan, typically obtained from {@link IPlanSource#snapshot()}
	 * @param now
	 *            reference instant for the {@code elapsedMs} computation when the plan is still in flight; on completed
	 *            plans this is ignored in favor of {@code completedAt}
	 * @return a freshly-built summary
	 */
	public static QueryPlanSummary of(QueryPlan plan, Instant now) {
		Counter counter = new Counter();
		// The plan's `nodes` list is already deduplicated by the projector — one entry per logical step — so this is
		// just a flat sweep, no DAG traversal needed. Iteration order is the projector's DFS-discovery order, which
		// keeps `latestCompletedLabel` deterministic across snapshots.
		for (QueryPlanNode n : plan.getNodes()) {
			count(counter, n);
		}

		long elapsed;
		if (plan.getCompletedAt() != null) {
			elapsed = Math.max(0L, plan.getCompletedAt().toEpochMilli() - plan.getSubmittedAt().toEpochMilli());
		} else {
			elapsed = Math.max(0L, now.toEpochMilli() - plan.getSubmittedAt().toEpochMilli());
		}

		long startDelay;
		if (plan.getExecutionStartedAt() != null) {
			startDelay =
					Math.max(0L, plan.getExecutionStartedAt().toEpochMilli() - plan.getSubmittedAt().toEpochMilli());
		} else {
			// Still queued — report the wait so far; it'll grow on each poll until execution begins.
			startDelay = Math.max(0L, now.toEpochMilli() - plan.getSubmittedAt().toEpochMilli());
		}

		return QueryPlanSummary.builder()
				.state(plan.getState())
				.totalNodes(counter.total)
				.doneNodes(counter.done)
				.pendingNodes(counter.pending)
				.runningNodes(counter.running)
				.failedNodes(counter.failed)
				.elapsedMs(elapsed)
				.startDelayMs(startDelay)
				.totalRowsOut(counter.rowsOut)
				.latestCompletedLabel(counter.latestCompletedLabel)
				.build();
	}

	/**
	 * Business logic for the summary aggregation — given the {@link Counter} state and a node, update the counters.
	 * Extracted from the walk for symmetry with {@link #walk(QueryPlanNode, Consumer)} and to make per-node logic
	 * testable in isolation.
	 */
	// Switch is exhaustive over NodeState; PMD warns when an exhaustive switch carries a default. Checkstyle's
	// MissingSwitchDefault fires for statement-style switches without one — suppress it here.
	@SuppressWarnings("checkstyle:MissingSwitchDefault")
	protected static void count(Counter counter, QueryPlanNode n) {
		counter.total++;
		switch (n.getState()) {
		case DONE -> {
			counter.done++;
			if (n.getStats().getRowsOut() != null) {
				counter.rowsOut += n.getStats().getRowsOut();
			}
			Instant completedAt = n.getStats().getCompletedAt();
			if (completedAt != null
					&& (counter.latestCompletedAt == null || completedAt.isAfter(counter.latestCompletedAt))) {
				counter.latestCompletedAt = completedAt;
				counter.latestCompletedLabel = n.getLabel();
			}
		}
		case PENDING -> counter.pending++;
		case RUNNING -> counter.running++;
		case FAILED -> counter.failed++;
		case SKIPPED -> {
			// not counted in any bucket — surface in a follow-up if needed
		}
		}
	}

	/** Internal mutable accumulator — avoids stacking boxed-longs through the recursion. */
	protected static final class Counter {
		long total;
		long done;
		long pending;
		long running;
		long failed;
		long rowsOut;
		@Nullable
		Instant latestCompletedAt;
		@Nullable
		String latestCompletedLabel;
	}
}
