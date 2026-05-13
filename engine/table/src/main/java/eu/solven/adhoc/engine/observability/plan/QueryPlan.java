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
import java.util.UUID;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.query.AdhocQueryId;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;
import lombok.experimental.NonFinal;

/**
 * Top-level container for the plan of one {@code execute(...)} call. Composite-cube fan-outs produce one
 * {@link QueryPlan} per sub-cube; their {@link #parentQueryId} links back to the root execution.
 *
 * <p>
 * Field semantics:
 * <ul>
 * <li>{@link #queryId} — the {@link AdhocQueryId} of this execution. Registry key.</li>
 * <li>{@link #parentQueryId} — non-null when this plan was spawned from a parent {@code SUB_CUBE_DELEGATION} node. UI
 * walks parent-&gt;child links via {@link IQueryPlanRegistry#getChildrenOf(AdhocQueryId)}.</li>
 * <li>{@link #submittedCustomMarker} — recorded once at submission time. Per-node marker decisions (routing measures,
 * composite translations) will live on nodes in a follow-up PR.</li>
 * <li>{@link #root} — root of the DAG of {@link QueryPlanNode}s for this plan.</li>
 * <li>{@link #nodeCount} — total live nodes anywhere under {@link #root}. Used by the registry's size-capped eviction
 * policy (we cap by node count, not by plan count, so light queries accumulate freely while a single 20k-node plan eats
 * its fair share of the budget).</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
public class QueryPlan {
	@NonNull
	AdhocQueryId queryId;

	/**
	 * Just the UUID of the parent execution, matching {@link AdhocQueryId#getParentQueryId()} on the engine side. UI
	 * navigation: {@code registry.getChildrenOf(parentAdhocQueryId)} compares this against the parent's own
	 * {@code AdhocQueryId.queryId}. Synthesizing a full {@code AdhocQueryId} here would require knowing the parent's
	 * cube/queryHash/etc., which isn't available at sub-cube spawn time.
	 */
	@Nullable
	UUID parentQueryId;

	@NonNull
	String cubeName;

	@Nullable
	Object submittedCustomMarker;

	/**
	 * Aggregate plan state. Mutable for the same in-place reason as {@link QueryPlanNode#getState()}: the registry
	 * updater flips it on {@code PlanCompleted} / {@code PlanFailed} without rebuilding the plan.
	 */
	@NonFinal
	@Setter
	@NonNull
	@Builder.Default
	PlanState state = PlanState.PENDING;

	@NonNull
	Instant submittedAt;

	/**
	 * When the engine actually started executing this plan — typically a few millis after {@link #submittedAt} on an
	 * idle system, but possibly seconds or minutes later when the query pool is saturated. Mutable so the engine can
	 * flip it in place at the start of execution without rebuilding the plan. {@code null} while the plan is still
	 * queued.
	 */
	@NonFinal
	@Setter
	@Nullable
	Instant executionStartedAt;

	/** Mutable — set when the plan reaches a terminal {@link PlanState}. */
	@NonFinal
	@Setter
	@Nullable
	Instant completedAt;

	@NonNull
	QueryPlanNode root;

	/**
	 * Pre-computed node count for the {@link IQueryPlanRegistry} eviction policy. Callers populating the plan are
	 * expected to set this; the registry trusts it. Off by one or two from the actual live size in progress is
	 * acceptable — the cap is a guideline, not a hard limit.
	 */
	long nodeCount;
}
