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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import eu.solven.adhoc.engine.step.IHasMeasure;
import eu.solven.adhoc.filter.IHasFilters;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.model.query.IGroupBy;
import eu.solven.adhoc.model.query.IHasCustomMarker;
import eu.solven.adhoc.model.query.IHasGroupBy;
import eu.solven.adhoc.model.query.IHasMeasures;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.options.IQueryOption;
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
	private static final Object SYNTHETIC_ROOT = new Object();

	/**
	 * Structured per-property view of a plan participant — a step ({@code CubeQueryStep}, {@code TableQueryStep}) or a
	 * top-level query ({@code CubeQuery}) — intended for renderers (Mermaid graph labels, log formatters).
	 *
	 * <p>
	 * Returns a {@link LinkedHashMap} so the iteration order matches the property order a reader expects: measure(s)
	 * first, then {@code filter}, {@code groupBy}, {@code customMarker}, {@code options}. Optional properties are
	 * omitted (matchAll filter, grand-total groupBy, null customMarker, empty options) — same conditional inclusion as
	 * {@code toString} implementations on the participating types, so the two representations stay aligned.
	 *
	 * <p>
	 * Polymorphic on purpose: the input is typed as {@link Object} and the method picks up whichever capability
	 * interfaces the value implements. {@link IHasMeasure} (singular, per-step) and {@link IHasMeasures} (plural,
	 * per-query) are mutually exclusive in practice; both are checked so the helper works for either.
	 *
	 * @param participant
	 *            either a {@code CubeQueryStep}/{@code TableQueryStep} (singular measure) or a {@code CubeQuery}
	 *            (plural measures). May implement none, any, or all of the capability interfaces — missing capabilities
	 *            just skip the corresponding entry.
	 * @return a fresh, mutable map — callers may freely add additional entries
	 */
	public static Map<String, String> toDetails(Object participant) {
		Map<String, String> details = new LinkedHashMap<>();

		if (participant instanceof IHasMeasure hasMeasure) {
			details.put("measure", String.valueOf(hasMeasure.getMeasure()));
		} else if (participant instanceof IHasMeasures hasMeasures) {
			// Plural shape: a comma-joined list of measure names wrapped in `[…]`. Distinct field name from the
			// singular case so a reader can tell a per-step view ("measure=…") from a per-query view
			// ("measures=[a, b]") at a glance.
			details.put("measures",
					hasMeasures.getMeasures()
							.stream()
							.map(IMeasure::getName)
							.collect(java.util.stream.Collectors.joining(", ", "[", "]")));
		}
		if (participant instanceof IHasFilters hasFilters) {
			ISliceFilter filter = hasFilters.getFilter();
			// null-guard: production types initialise filter to MATCH_ALL via @NonNull defaults, but Mockito-built
			// fixtures (used in unit tests) leave it null when no stubbing is set up. Treat null as matchAll.
			if (filter != null && !filter.isMatchAll()) {
				details.put("filter", String.valueOf(filter));
			}
		}
		if (participant instanceof IHasGroupBy hasGroupBy) {
			IGroupBy groupBy = hasGroupBy.getGroupBy();
			// Same null-guard rationale as above.
			if (groupBy != null && !groupBy.isGrandTotal()) {
				details.put("groupBy", String.valueOf(groupBy));
			}
		}
		if (participant instanceof IHasCustomMarker hasCustomMarker) {
			hasCustomMarker.optCustomMarker().ifPresent(customMarker -> {
				details.put("customMarker", String.valueOf(customMarker));
			});
		}
		if (participant instanceof IHasQueryOptions hasOptions) {
			Set<IQueryOption> options = hasOptions.getOptions();
			if (!options.isEmpty()) {
				details.put("options", String.valueOf(options));
			}
		}
		return details;
	}

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
		return project(dag,
				queryId,
				parentQueryId,
				cubeName,
				Collections.emptyMap(),
				submittedAt,
				executionStartedAt,
				planState,
				completedAt,
				Collections.emptyMap());
	}

	/**
	 * Back-compat overload (no {@code cubeQueryDetails}) — callers that have not yet been updated to surface the
	 * submitted query's structured view get a bare {@code CUBE_QUERY} top-level node with no detail breakdown.
	 */
	public QueryPlan project(QueryStepsDag dag,
			AdhocQueryId queryId,
			@Nullable UUID parentQueryId,
			String cubeName,
			Instant submittedAt,
			@Nullable Instant executionStartedAt,
			PlanState planState,
			@Nullable Instant completedAt,
			Map<Object, List<QueryPlanNode>> fragments) {
		return project(dag,
				queryId,
				parentQueryId,
				cubeName,
				Collections.emptyMap(),
				submittedAt,
				executionStartedAt,
				planState,
				completedAt,
				fragments);
	}

	/**
	 * Project variant accepting a fragments map. The map is keyed by anchor subject; each value is the list of subtrees
	 * to graft as additional children of any node whose {@code subject.equals(anchor)}. Subtrees are also walked
	 * recursively so deeper anchors (e.g. a SQL leaf anchored on a TableQueryV4 that was itself grafted under a
	 * TableQueryStep) attach transparently.
	 *
	 * @param fragments
	 *            anchor → subtrees map; pass {@link Collections#emptyMap()} when no enrichment is available
	 */
	// Parameter list is wide on purpose — every argument names a stable plan attribute that the projector emits into
	// the resulting {@link QueryPlan} as-is. Packing them into a single value object would just shift the same fields
	// around without simplifying anything.
	@SuppressWarnings("PMD.ExcessiveParameterList")
	public QueryPlan project(QueryStepsDag dag,
			AdhocQueryId queryId,
			@Nullable UUID parentQueryId,
			String cubeName,
			Map<String, String> cubeQueryDetails,
			Instant submittedAt,
			@Nullable Instant executionStartedAt,
			PlanState planState,
			@Nullable Instant completedAt,
			Map<Object, List<QueryPlanNode>> fragments) {
		ProjectionState ctx = new ProjectionState(dag.getStepToCost(), fragments);

		// Always wrap the per-cube-root nodes under a top-level CUBE_QUERY node — the user-facing "query"
		// in the plan registry. The CubeQuery's structured detail breakdown (measures / filter / groupBy /
		// customMarker / options) travels through `cubeQueryDetails` so the SPA can render it as a
		// multi-line label, mirroring the per-step rendering.
		NodeState rootState;
		if (planState == PlanState.PENDING) {
			rootState = NodeState.PENDING;
		} else {
			rootState = NodeState.DONE;
		}
		String rootLabel;
		if (cubeName.isEmpty()) {
			rootLabel = "CubeQuery";
		} else {
			rootLabel = "CubeQuery on " + cubeName;
		}
		String rootId = ctx.emit(SYNTHETIC_ROOT,
				QueryPlanNode.builder()
						.subject(SYNTHETIC_ROOT)
						.operator(NodeOperator.CUBE_QUERY)
						.label(rootLabel)
						.details(Map.copyOf(cubeQueryDetails))
						.state(rootState)
						.build());

		// Materialize each cube root and connect it to the CUBE_QUERY wrapper.
		for (CubeQueryStep root : dag.getRoots()) {
			String childId = ctx.visitCubeStep(root, dag);
			ctx.addEdge(rootId, childId);
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
				.rootId(rootId)
				.nodes(List.copyOf(ctx.nodes))
				.edges(List.copyOf(ctx.edges))
				.nodeCount(ctx.nodes.size())
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

	/**
	 * Mutable accumulator for one {@code project} call — collects the deduped {@link QueryPlanNode} list, the
	 * {@link QueryPlanEdge} list, and the {@code subject → id} dedup map. Encapsulating the state in one class keeps
	 * the recursive helpers (cube-step walk, fragment walk) signature-clean.
	 *
	 * <p>
	 * The {@code subjectToId} map is the cycle guard: any revisit of an already-allocated subject returns the existing
	 * id without recursing, breaking infinite loops naturally (no need for an {@code inProgress} stack like the
	 * previous tree-based projector required). It is value-equality-based ({@code HashMap}, not
	 * {@code IdentityHashMap}) because two distinct Java instances with the same {@code subject.equals(...)} value
	 * represent the same logical step — the projector's whole point is to recognise that.
	 */
	protected static final class ProjectionState {
		final Map<ICubeQueryStep, SizeAndDuration> stepToCost;
		final Map<Object, List<QueryPlanNode>> fragments;
		final Map<Object, String> subjectToId = new LinkedHashMap<>();
		final List<QueryPlanNode> nodes = new ArrayList<>();
		final List<QueryPlanEdge> edges = new ArrayList<>();

		ProjectionState(Map<ICubeQueryStep, SizeAndDuration> stepToCost, Map<Object, List<QueryPlanNode>> fragments) {
			this.stepToCost = stepToCost;
			this.fragments = fragments;
		}

		/**
		 * Allocate a stable id for {@code subject} (if not already allocated) and store the supplied {@code template}
		 * with that id in {@link #nodes}. Returns the id. If the subject already has an id, returns it without storing
		 * — the first emit wins.
		 */
		String emit(Object subject, QueryPlanNode template) {
			String existing = subjectToId.get(subject);
			if (existing != null) {
				return existing;
			}
			String id = "n" + nodes.size();
			subjectToId.put(subject, id);
			nodes.add(template.toBuilder().id(id).children(Collections.emptyList()).build());
			return id;
		}

		void addEdge(String parentId, String childId) {
			edges.add(QueryPlanEdge.builder().parentId(parentId).childId(childId).build());
		}

		/**
		 * Walk the cube DAG starting from {@code step}. Materializes one {@link QueryPlanNode} per unique
		 * {@link CubeQueryStep} (dedup by step equality), recursing into outgoing edges and applying any anchored
		 * fragments. Returns the id of the node representing {@code step}.
		 */
		String visitCubeStep(CubeQueryStep step, QueryStepsDag dag) {
			String existing = subjectToId.get(step);
			if (existing != null) {
				return existing;
			}
			SizeAndDuration cost = stepToCost.get(step);
			NodeState state;
			if (cost == null) {
				state = NodeState.PENDING;
			} else {
				state = NodeState.DONE;
			}
			NodeStats stats;
			if (cost == null) {
				stats = NodeStats.empty();
			} else {
				stats = NodeStats.builder()
						.rowsOut(cost.getSize())
						.elapsedMs(Math.max(0L, cost.getDuration().toMillis()))
						.build();
			}
			String id = emit(step,
					QueryPlanNode.builder()
							.subject(step)
							.operator(NodeOperator.CUBE_STEP)
							// Structured per-property view drives the SPA's multi-line label. Headline is the measure
							// name so log renderers (which read `label` only) still produce useful output.
							.label(String.valueOf(step.getMeasure()))
							.details(toDetails(step))
							.state(state)
							.stats(stats)
							.build());

			for (DefaultEdge edge : dag.getInducedToInducer().outgoingEdgesOf(step)) {
				CubeQueryStep child = Graphs.getOppositeVertex(dag.getInducedToInducer(), edge, step);
				addEdge(id, visitCubeStep(child, dag));
			}

			// Apply fragments anchored on this step's subject (table engine V4 grafts, induced TABLE_STEP grafts).
			appendFragmentEdges(id, step);

			return id;
		}

		/**
		 * For each fragment anchored on {@code anchorSubject}, allocate it (idempotent) and add a parent → fragment
		 * edge. The fragment's own children (publisher-built subtree) are walked recursively.
		 *
		 * <p>
		 * Self-edge guard: when the fragment's subject equals the anchor (publishers like the induced-step path used to
		 * do this — fragment anchored on X with subject = X), {@link #visitFragment} returns the parent's own id and
		 * we'd otherwise emit a parent → parent edge. The cube-step node already IS the fragment, so the edge is
		 * meaningless; skip it rather than dirty the graph with a self-loop.
		 */
		void appendFragmentEdges(String parentId, Object anchorSubject) {
			List<QueryPlanNode> grafts = fragments.get(anchorSubject);
			if (grafts == null || grafts.isEmpty()) {
				return;
			}
			for (QueryPlanNode graft : grafts) {
				String graftId = visitFragment(graft);
				if (!graftId.equals(parentId)) {
					addEdge(parentId, graftId);
				}
			}
		}

		/**
		 * Materialize a fragment (a publisher-built subtree). Dedup by subject — a fragment shared between several
		 * anchors appears once in {@link #nodes} with edges from each parent. The fragment's {@code children} list is
		 * walked recursively, and any fragments anchored on the fragment's own subject (V4 → SQL leaf chain) are
		 * grafted too.
		 */
		String visitFragment(QueryPlanNode fragment) {
			Object subject = fragment.getSubject();
			String existing = subjectToId.get(subject);
			if (existing != null) {
				return existing;
			}
			String id = emit(subject, fragment);
			for (QueryPlanNode child : fragment.getChildren()) {
				addEdge(id, visitFragment(child));
			}
			appendFragmentEdges(id, subject);
			return id;
		}
	}
}
