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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.query.AdhocQueryId;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * Live {@link IPlanSource} backed by a running {@link QueryStepsDag}. Each {@link #snapshot()} call re-projects the
 * dag's current state into a fresh immutable {@link QueryPlan} via {@link QueryPlanProjector}.
 *
 * <p>
 * The engine creates one source per execution, retains it in the registry, and notifies completion via
 * {@link #markCompleted(PlanState, Instant)}. Snapshot is callable at any point — before completion (live view) or
 * after (post-mortem inspection while the registry hasn't evicted yet).
 *
 * <p>
 * Mid-flight projection — i.e. one node DONE while a downstream node is still running — is covered by
 * {@code TestLiveQueryPlanSource_MidFlight} (engine/cube test) via a {@code CountDownLatch}-blocking combinator. The
 * test pins both the partial-stats correctness and the {@link #version()} bump on {@link #markCompleted}.
 *
 * @author Benoit Lacelle
 */
@Builder
public class LiveQueryPlanSource implements IPlanSource {

	@NonNull
	private final QueryStepsDag dag;

	@NonNull
	private final AdhocQueryId queryId;

	@Nullable
	private final UUID parentQueryId;

	@NonNull
	private final String cubeName;

	/**
	 * Structured per-property view of the submitted {@code CubeQuery} (measures / filter / groupBy / customMarker /
	 * options). Populated by the engine when the source is registered, so the projector can render it as the top-level
	 * {@code CUBE_QUERY} node without depending on the cube-package {@code CubeQuery} type. Empty when the caller has
	 * no structured view to offer — the projector then degrades to a bare top-level node with no detail breakdown.
	 */
	@NonNull
	@Default
	private final Map<String, String> cubeQueryDetails = Collections.emptyMap();

	@NonNull
	private final Instant submittedAt;

	/**
	 * Projector used to materialize a {@link QueryPlan} on each {@link #snapshot()}. Defaults to a vanilla
	 * {@link QueryPlanProjector}; tests / specialized embedders can inject a subclass to add per-node enrichment (e.g.
	 * labels derived from the engine's tabular optimizer state).
	 */
	@NonNull
	@Default
	private final QueryPlanProjector projector = new QueryPlanProjector();

	/**
	 * Bumped on each state transition the engine can observe (typically the {@code stepToCost.put(...)} site in
	 * {@code QueryStepsDag.registerExecutionFeedback}). Engine wiring (deferred) calls {@link #bumpVersion()} on each
	 * transition; until then the counter stays at its initial value and pollers redo work on every call — acceptable
	 * since the engine doesn't drive PR 3 in this commit.
	 */
	@NonNull
	@Builder.Default
	private final AtomicLong versionCounter = new AtomicLong();

	/**
	 * Plan-level state. {@link AtomicReference} so {@link #snapshot()} reads a coherent value even while
	 * {@link #markCompleted(PlanState, Instant)} is happening concurrently.
	 */
	@NonNull
	@Builder.Default
	private final AtomicReference<PlanState> planState = new AtomicReference<>(PlanState.PENDING);

	/** Set on completion. {@code null} while in-flight. {@link AtomicReference} provides cross-thread visibility. */
	@NonNull
	@Builder.Default
	private final AtomicReference<@Nullable Instant> completedAt = new AtomicReference<>();

	/**
	 * Set when execution actually starts (after queueing). {@code null} while the query is still waiting for a slot —
	 * the gap {@code executionStartedAt - submittedAt} feeds {@link QueryPlanSummary#getStartDelayMs()}, which surfaces
	 * pool-saturation symptoms.
	 */
	@NonNull
	@Builder.Default
	private final AtomicReference<@Nullable Instant> executionStartedAt = new AtomicReference<>();

	/**
	 * Fragments published via {@link IQueryPlanRegistry#publishFragment(AdhocQueryId, Object, QueryPlanNode)}. Keyed by
	 * anchor (the subject the projector will match against existing nodes); each anchor accumulates a list of subtrees,
	 * deduplicated by the subtree's root subject so that re-publishing the same fragment (e.g. a wrapper re-rendering
	 * identical SQL) replaces rather than appends.
	 *
	 * <p>
	 * {@link ConcurrentHashMap} is sufficient — writes happen on the engine's per-tableQuery worker threads, reads
	 * happen on the snapshot path. The per-anchor list is replaced atomically on every write (copy-on-write), so
	 * readers never see a torn list.
	 */
	@NonNull
	@Builder.Default
	private final Map<Object, List<QueryPlanNode>> fragmentsByAnchor = new ConcurrentHashMap<>();

	@Override
	public AdhocQueryId getQueryId() {
		return queryId;
	}

	@Override
	public QueryPlan snapshot() {
		return projector.project(dag,
				queryId,
				parentQueryId,
				cubeName,
				cubeQueryDetails,
				submittedAt,
				executionStartedAt.get(),
				planState.get(),
				completedAt.get(),
				snapshotFragments());
	}

	/**
	 * Return a deep-enough snapshot of the fragment map for the projector to use without seeing concurrent mutations
	 * mid-walk. Each per-anchor list is already copy-on-write (replaced wholesale on every write), so a single read of
	 * each entry gives us a stable {@link List} reference for the duration of this snapshot.
	 */
	protected Map<Object, List<QueryPlanNode>> snapshotFragments() {
		if (fragmentsByAnchor.isEmpty()) {
			return Collections.emptyMap();
		}
		// Copy the entry references; the lists themselves are immutable views (replaced on write).
		return Map.copyOf(fragmentsByAnchor);
	}

	@Override
	public long version() {
		return versionCounter.get();
	}

	@Override
	public boolean isCompleted() {
		PlanState s = planState.get();
		return s == PlanState.DONE || s == PlanState.FAILED;
	}

	/**
	 * Called by the engine when a step state transition has happened — the next {@link #snapshot()} may differ from the
	 * previous one. Pollers use the returned version to short-circuit when nothing has changed.
	 *
	 * @return the new version
	 */
	public long bumpVersion() {
		return versionCounter.incrementAndGet();
	}

	/**
	 * Called by the engine when execution actually starts (after any pool-saturation queueing). Idempotent — the first
	 * call wins so the recorded delay isn't reset if the engine re-enters this path.
	 *
	 * @param at
	 *            wall-clock time at which execution started
	 */
	public void markExecutionStarted(Instant at) {
		if (executionStartedAt.compareAndSet(null, at)) {
			// Flip PENDING → RUNNING in the same step. Without this, planState stays PENDING all the way until
			// markCompleted(...), so a mid-flight snapshot reports state=PENDING while half the dag is DONE — the UI
			// then renders the "Queued" badge for a query that is in fact running.
			planState.compareAndSet(PlanState.PENDING, PlanState.RUNNING);
			bumpVersion();
		}
	}

	/**
	 * Called by the engine when the plan reaches a terminal state. Idempotent — subsequent calls with the same
	 * arguments are no-ops; subsequent calls with conflicting state are accepted (the latest wins, but in practice the
	 * engine only calls this once).
	 *
	 * @param state
	 *            {@link PlanState#DONE} on success, {@link PlanState#FAILED} on exception
	 * @param at
	 *            when the plan completed
	 */
	public void markCompleted(PlanState state, Instant at) {
		planState.set(state);
		completedAt.set(at);
		bumpVersion();
	}

	/**
	 * Publish a fragment under {@code anchor}. Called by the registry on behalf of any layer that wants to enrich the
	 * plan (typically the table engine and table wrappers). Deduplicates fragments whose subtree root carries the same
	 * {@code subject} as a previously published one for the same anchor — replaces rather than appends so a wrapper
	 * re-rendering identical SQL on a retry doesn't grow the fragment list unboundedly.
	 *
	 * @param anchor
	 *            the subject value the projector will match against existing nodes when grafting
	 * @param subtree
	 *            the fragment to graft; its {@code subject} is used as the dedup key within the anchor's list
	 */
	public void publishFragment(Object anchor, QueryPlanNode subtree) {
		// Copy-on-write list update so concurrent readers see either the old list or the new list, never a torn one.
		// `compute` is atomic on ConcurrentHashMap, so two threads racing on the same anchor serialise here.
		fragmentsByAnchor.compute(anchor, (a, existing) -> {
			List<QueryPlanNode> base;
			if (existing == null) {
				base = List.of();
			} else {
				base = existing;
			}
			List<QueryPlanNode> next = new ArrayList<>(base.size() + 1);
			Object incomingSubject = subtree.getSubject();
			for (QueryPlanNode prev : base) {
				if (!prev.getSubject().equals(incomingSubject)) {
					next.add(prev);
				}
			}
			next.add(subtree);
			return Collections.unmodifiableList(next);
		});
		bumpVersion();
	}
}
