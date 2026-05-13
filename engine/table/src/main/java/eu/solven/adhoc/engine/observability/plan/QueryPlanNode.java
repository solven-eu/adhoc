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

import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.query.AdhocQueryId;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;
import lombok.experimental.NonFinal;

/**
 * One node in a {@link QueryPlan}. Mutates during execution: {@link #state} transitions and {@link #stats} fills in.
 * Mutation happens on the engine-owner thread; UI consumers always read via
 * {@link IQueryPlanRegistry#snapshot(AdhocQueryId)} which returns a deep-copy.
 *
 * <p>
 * The functional identity of the node is carried by {@link #subject} — typically a {@code CubeQueryStep},
 * {@code TableQueryStep}, {@code TableQueryV*} or other execution-step object whose own {@code hashCode} /
 * {@code equals} / {@code toString} contracts make it identifiable. The plan is a DAG (a single step may have several
 * parents in dedup'd cases); equality of nodes within a plan reduces to equality of their subjects.
 * {@link AdhocQueryId} disambiguates same-shaped subjects across different executions and lives at the
 * {@link QueryPlan} level, not on the node.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
public class QueryPlanNode {
	/**
	 * The functional object this node represents (a {@code CubeQueryStep}, {@code TableQueryStep}, etc.). Provides
	 * identity (hashCode/equals) and a human-readable {@code toString} for the log renderer. Polymorphic on purpose —
	 * the plan model does not constrain which step kinds the engine produces.
	 */
	@NonNull
	Object subject;

	/**
	 * Operator kind, used by the log renderer + UI to pick an icon / label. Independent of the runtime class of
	 * {@link #subject} so the engine can group several step types under one display category if useful.
	 */
	@NonNull
	NodeOperator operator;

	/**
	 * Display label, defaults to {@code subject.toString()}. UI grids show this when there is no space for the subject.
	 */
	@NonNull
	String label;

	/**
	 * Child nodes — the steps this node depends on. Read-only view; build via {@code toBuilder} when assembling a plan.
	 * A given child may appear under multiple parents (DAG-style); equality of children is by their subjects.
	 */
	@Builder.Default
	@NonNull
	List<QueryPlanNode> children = Collections.emptyList();

	/**
	 * Sub-cube delegation pointer — populated when {@link #operator} is {@link NodeOperator#SUB_CUBE_DELEGATION}. The
	 * actual sub-plan lives in the registry under this id; UI walks the link to render the nested plan.
	 */
	@Nullable
	AdhocQueryId subQueryId;

	/**
	 * Current execution state. Mutable — the event-driven registry updater flips this in-place on each
	 * {@code NodeStarted} / {@code NodeCompleted} event. Reads via
	 * {@link IQueryPlanRegistry#snapshot(eu.solven.adhoc.query.AdhocQueryId)} deep-copy so concurrent reads never
	 * observe a mid-mutation tree.
	 */
	@Builder.Default
	@NonFinal
	@Setter
	@NonNull
	NodeState state = NodeState.PENDING;

	/**
	 * Stats — {@link NodeStats#empty()} until execution starts. Mutable for the same reason as {@link #state};
	 * {@link NodeStats} itself is immutable so updates allocate a fresh instance.
	 */
	@Builder.Default
	@NonFinal
	@Setter
	@NonNull
	NodeStats stats = NodeStats.empty();

	/**
	 * Free-form key→value details for renderer-only fields the structured model does not yet capture (e.g. the specific
	 * column-store strategy chosen, the pruned-column set on a join). Kept open so PR 1 doesn't need to enumerate every
	 * observable; richer typed slots can be promoted out of this map later.
	 *
	 * <p>
	 * Customer-marker decisions ({@code RoutingMeasure} branch chosen, composite-cube marker translation) will move
	 * into a dedicated structured slot in a follow-up PR — they are intentionally NOT in this map.
	 */
	@Builder.Default
	@NonNull
	java.util.Map<String, String> details = Collections.emptyMap();
}
