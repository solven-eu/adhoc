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

import eu.solven.adhoc.query.AdhocQueryId;

/**
 * Source of {@link QueryPlan} snapshots — the pull-side abstraction the registry holds.
 *
 * <p>
 * The canonical implementation is {@link LiveQueryPlanSource}: a live-projection source backed by a running
 * {@code QueryStepsDag} that returns a fresh immutable {@link QueryPlan} on each {@link #snapshot()}, projected from
 * the dag's current state. Cheap when nobody calls, O(N) per call when polled — for the 20k-step plans this codebase
 * produces, single-digit ms per snapshot on modern hardware.
 *
 * <p>
 * Snapshot semantics are intentionally <em>monitor-quality</em>: there is no ACID-style guarantee that the returned
 * tree reflects a single instant — concurrent engine mutation may produce a tree in which one node has its
 * post-completion stats while a sibling is still pending. Same trade-off as {@code ConcurrentHashMap.entrySet()}.
 * Document and move on.
 *
 * @author Benoit Lacelle
 */
public interface IPlanSource {
	/**
	 * @return the {@link AdhocQueryId} this source pertains to. Stable across {@link #snapshot()} calls.
	 */
	AdhocQueryId getQueryId();

	/**
	 * Produce a {@link QueryPlan} reflecting the source's current view. Implementations are free to return the same
	 * instance every time (static case) or a fresh tree per call (projection case) — callers must not assume either.
	 *
	 * @return the current plan
	 */
	QueryPlan snapshot();

	/**
	 * Optional change counter — bumps when the underlying source state changes. Used by pollers to short-circuit an
	 * unchanged snapshot.
	 *
	 * <p>
	 * Default returns {@code 0} for sources whose state never changes. Live sources should bump it whenever the
	 * projected plan would differ from the previous {@link #snapshot()} return — typically once per step state
	 * transition. The counter is monotonic but otherwise opaque; callers compare equality, not order.
	 *
	 * @return the current version
	 */
	default long version() {
		return 0L;
	}

	/**
	 * @return {@code true} once the source's plan has reached a terminal state ({@link PlanState#DONE} or
	 *         {@link PlanState#FAILED}). The registry uses this to decide when an entry becomes eligible for LRU
	 *         eviction. {@link #snapshot()} is still callable after a source is completed — callers can pull a
	 *         post-mortem of the plan as long as the registry holds it.
	 */
	boolean isCompleted();

	/**
	 * Cheap status summary — what a poller needs for a "this query has been running for a while, what's happening?"
	 * status chip. Counts node states, sums {@code rowsOut}, names the most-recently completed node. Designed for
	 * higher-frequency polling than {@link #snapshot()}: a UI status line can refresh at 2 Hz without paying for a full
	 * immutable plan-tree rebuild.
	 *
	 * <p>
	 * The default implementation goes through {@link #snapshot()} + {@link QueryPlanSummary#of}. Live sources MAY
	 * override to short-circuit the immutable-tree allocation when only the summary is needed.
	 *
	 * @return the current summary, fresh per call
	 */
	default QueryPlanSummary summary() {
		return QueryPlanSummary.of(snapshot(), java.time.Instant.now());
	}
}
