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
import java.util.Collections;
import java.util.List;
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
 * <li>{@link #rootId} — id of the {@code CUBE_QUERY} node sitting at the top of the plan graph (always the
 * {@code CubeQuery} wrapper produced by {@code QueryPlanProjector}). Resolve via
 * {@code nodes.stream().filter(n -> rootId.equals(n.getId())).findFirst()}.</li>
 * <li>{@link #nodes} — flat list of every {@link QueryPlanNode} reachable from {@link #rootId}. Each node appears
 * exactly once even when several parents depend on it (DAG: shared TableQueryV4, shared SQL leaf, …). Stable order:
 * projector emits in DFS-discovery order, so {@code rootId} is at index 0.</li>
 * <li>{@link #edges} — flat list of every parent → child link. Endpoints reference {@link QueryPlanNode#getId()}.
 * Renderers reconstruct the graph by indexing nodes by id then walking edges; tree-shaped UIs (Mermaid, ASCII
 * explainers) can BFS / DFS from {@link #rootId}.</li>
 * <li>{@link #nodeCount} — total node count (equals {@code nodes.size()}). Used by the registry's size-capped eviction
 * policy (we cap by node count, not by plan count, so light queries accumulate freely while a single 20k-node plan eats
 * its fair share of the budget).</li>
 * </ul>
 *
 * <p>
 * Why graph (not tree): the projector memoizes per-subject so a step shared between multiple parents materializes once.
 * Before this shape, Jackson serialization re-flattened the DAG into a tree by emitting the shared node once per
 * occurrence — a single TableQueryV4 served by 3 induced steps appeared 3 times in the JSON, costing payload size and
 * forcing UI consumers to re-dedup at parse time. The flat {@code nodes}/{@code edges} shape is the natural encoding of
 * the DAG.
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

	/** Id of the {@code CUBE_QUERY} wrapper at the top of the plan graph. Always present in {@link #nodes}. */
	@NonNull
	String rootId;

	/**
	 * Flat list of every node in the plan, deduplicated by subject. See class-level doc for ordering.
	 */
	@NonNull
	@Builder.Default
	List<QueryPlanNode> nodes = Collections.emptyList();

	/** Flat list of every parent → child link. Endpoints reference {@link QueryPlanNode#getId()}. */
	@NonNull
	@Builder.Default
	List<QueryPlanEdge> edges = Collections.emptyList();

	/**
	 * Pre-computed node count for the {@link IQueryPlanRegistry} eviction policy. Callers populating the plan are
	 * expected to set this; the registry trusts it. Off by one or two from the actual live size in progress is
	 * acceptable — the cap is a guideline, not a hard limit.
	 */
	long nodeCount;
}
