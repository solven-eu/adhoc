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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ICubeQueryStep;
import eu.solven.adhoc.query.AdhocQueryId;

/**
 * Pure projection from a live {@link QueryStepsDag} (plus query metadata) to an immutable {@link QueryPlan} tree.
 *
 * <p>
 * Used by the pull-side {@code LiveQueryPlanSource} on every {@code snapshot()}: walks the dag's
 * {@code inducedToInducer} graph, materializes one {@link QueryPlanNode} per {@link CubeQueryStep}, fills in
 * {@link NodeStats} from {@link QueryStepsDag#getStepToCost()} (which the engine populates in place as steps finish).
 * Nodes whose step is absent from {@code stepToCost} are reported as {@link NodeState#PENDING} — today's engine doesn't
 * expose a separate "running" set, so PENDING covers both not-yet-started AND in-progress. A follow-up engine change
 * can add an explicit running-set to disambiguate; the projector's shape doesn't have to change.
 *
 * <p>
 * Consistency: reads {@code stepToCost} (a {@code ConcurrentHashMap}) one entry at a time while the engine may
 * concurrently mutate. Two siblings can therefore disagree on "did I run before or after this other step?" — fine for
 * monitor-quality. Same trade-off as {@code ConcurrentHashMap.entrySet()}.
 *
 * @author Benoit Lacelle
 */
public class QueryPlanProjector {

	/**
	 * Marker used as the {@link QueryPlanNode#getSubject() subject} of the synthetic root when a plan has multiple
	 * roots.
	 */
	public static final Object SYNTHETIC_ROOT = new Object();

	/**
	 * Project the current state of {@code dag} into an immutable {@link QueryPlan}.
	 *
	 * <p>
	 * The {@code submittedCustomMarker} carried by the resulting {@link QueryPlan} is read from the dag's first
	 * {@link QueryStepsDag#getExplicits() explicit} step. The customMarker can vary per step (and per node in the
	 * resulting tree it remains accessible via {@code QueryPlanNode.getSubject()} when the subject is a
	 * {@link CubeQueryStep}); the plan-level field captures the marker at the entry of the dag, which matches the
	 * marker submitted with the query.
	 *
	 * @param dag
	 *            the live {@link QueryStepsDag} owned by the engine
	 * @param queryId
	 *            stable across the query's lifetime
	 * @param parentQueryId
	 *            non-null for composite-cube sub-cubes (their parent's queryId)
	 * @param cubeName
	 *            label for {@link QueryPlan#getCubeName()}
	 * @param submittedAt
	 *            when the query was submitted
	 * @param executionStartedAt
	 *            when the engine started executing this plan, or {@code null} while still queued (e.g. waiting for a
	 *            slot in a saturated pool)
	 * @param planState
	 *            aggregate state of the plan — the engine knows this from its own lifecycle; the projector takes it as
	 *            input rather than inferring (which would race with the engine completing the last step)
	 * @param completedAt
	 *            when the plan terminated, or {@code null} while still in flight
	 * @return a fresh immutable plan tree
	 */
	public QueryPlan project(QueryStepsDag dag,
			AdhocQueryId queryId,
			@Nullable UUID parentQueryId,
			String cubeName,
			Instant submittedAt,
			@Nullable Instant executionStartedAt,
			PlanState planState,
			@Nullable Instant completedAt) {
		Map<ICubeQueryStep, SizeAndDuration> stepToCost = dag.getStepToCost();

		// Memoize per-step node materialization to handle the DAG case where one step has multiple parents. We use
		// HashMap (CubeQueryStep has value-equals) rather than IdentityHashMap — same step reached via different
		// edges is the same node.
		Map<CubeQueryStep, QueryPlanNode> memo = new HashMap<>();
		Set<CubeQueryStep> stack = new LinkedHashSet<>(); // cycle guard; the dag should be acyclic but we don't assume
															// it

		List<QueryPlanNode> rootNodes = new ArrayList<>();
		for (CubeQueryStep root : dag.getRoots()) {
			rootNodes.add(materialize(root, dag, stepToCost, memo, stack));
		}

		// If there's more than one root, wrap them under a synthetic root carrying the plan-level metadata. With a
		// single root (the common case for a one-measure query), use it directly to avoid the extra layer.
		QueryPlanNode planRoot;
		if (rootNodes.size() == 1) {
			planRoot = rootNodes.get(0);
		} else {
			NodeState syntheticRootState;
			if (planState == PlanState.PENDING) {
				syntheticRootState = NodeState.PENDING;
			} else {
				syntheticRootState = NodeState.DONE;
			}
			planRoot = QueryPlanNode.builder()
					.subject(SYNTHETIC_ROOT)
					.operator(NodeOperator.CUBE_STEP)
					.label("(query roots × " + rootNodes.size() + ")")
					.children(List.copyOf(rootNodes))
					.state(syntheticRootState)
					.build();
		}

		// Synthetic-root contributes +1 to the node count when present.
		long nodeCount = memo.size();
		if (rootNodes.size() > 1) {
			nodeCount++;
		}

		return QueryPlan.builder()
				.queryId(queryId)
				.parentQueryId(parentQueryId)
				.cubeName(cubeName)
				.submittedCustomMarker(readSubmittedCustomMarker(dag))
				.state(planState)
				.submittedAt(submittedAt)
				.executionStartedAt(executionStartedAt)
				.completedAt(completedAt)
				.root(planRoot)
				.nodeCount(nodeCount)
				.build();
	}

	/**
	 * Read the plan-level customMarker from the dag's first explicit step. Returns {@code null} when the dag has no
	 * explicit step (defensive — should not happen on a real query).
	 */
	@Nullable
	protected Object readSubmittedCustomMarker(QueryStepsDag dag) {
		Iterator<CubeQueryStep> it = dag.getExplicits().iterator();
		if (it.hasNext()) {
			return it.next().getCustomMarker();
		}
		return null;
	}

	protected QueryPlanNode materialize(CubeQueryStep step,
			QueryStepsDag dag,
			Map<ICubeQueryStep, SizeAndDuration> stepToCost,
			Map<CubeQueryStep, QueryPlanNode> memo,
			Set<CubeQueryStep> stack) {
		QueryPlanNode cached = memo.get(step);
		if (cached != null) {
			return cached;
		}
		if (!stack.add(step)) {
			// Cycle: should not happen on a real DAG, but be safe. Return a self-referential-free leaf so the
			// projector doesn't loop forever in the presence of a corrupt graph.
			QueryPlanNode leaf = QueryPlanNode.builder()
					.subject(step)
					.operator(NodeOperator.CUBE_STEP)
					.label(step + " (cycle-detected)")
					.state(NodeState.PENDING)
					.build();
			memo.put(step, leaf);
			return leaf;
		}
		try {
			List<QueryPlanNode> children = new ArrayList<>();
			for (DefaultEdge edge : dag.getInducedToInducer().outgoingEdgesOf(step)) {
				CubeQueryStep child = Graphs.getOppositeVertex(dag.getInducedToInducer(), edge, step);
				children.add(materialize(child, dag, stepToCost, memo, stack));
			}

			SizeAndDuration cost = stepToCost.get(step);
			NodeState state;
			NodeStats stats;
			if (cost == null) {
				state = NodeState.PENDING;
				stats = NodeStats.empty();
			} else {
				state = NodeState.DONE;
				stats = NodeStats.builder()
						.rowsOut(cost.getSize())
						.elapsedMs(Math.max(0L, cost.getDuration().toMillis()))
						.build();
			}

			QueryPlanNode node = QueryPlanNode.builder()
					.subject(step)
					.operator(NodeOperator.CUBE_STEP)
					.label(String.valueOf(step))
					.children(List.copyOf(children))
					.state(state)
					.stats(stats)
					.build();
			memo.put(step, node);
			return node;
		} finally {
			stack.remove(step);
		}
	}
}
