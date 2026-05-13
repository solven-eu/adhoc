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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.query.AdhocQueryId;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.NonFinal;

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
 * Wiring TODO (deferred to a follow-up PR): {@code CubeQueryEngine.executeInScope} should build one of these right
 * after the DAG is constructed, hand it to {@code IQueryPlanRegistry.registerSource(...)}, and call
 * {@link #markCompleted(PlanState, Instant)} in its {@code finally} block. PR 3 introduces the abstractions; the engine
 * call sites move in PR 4.
 *
 * <p>
 * Future test reminder: a deterministic intermediate-state poll. Build a query whose dag includes a {@code Combinator}
 * backed by a {@code CountDownLatch} — the combinator blocks until the test latch is released, which gives the
 * assertion thread a chance to call {@link #snapshot()} and observe a mix of DONE / PENDING nodes mid-flight. Pin both
 * the partial-stats correctness and the version bump.
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
	private final AtomicLong version = new AtomicLong();

	/**
	 * Plan-level state. {@link AtomicReference} so {@link #snapshot()} reads a coherent value even while
	 * {@link #markCompleted(PlanState, Instant)} is happening concurrently.
	 */
	@NonNull
	@Builder.Default
	private final AtomicReference<PlanState> planState = new AtomicReference<>(PlanState.PENDING);

	/** Set on completion. {@code null} while in-flight. */
	@NonFinal
	@Nullable
	private volatile Instant completedAt;

	/**
	 * Set when execution actually starts (after queueing). {@code null} while the query is still waiting for a slot —
	 * the gap {@code executionStartedAt - submittedAt} feeds {@link QueryPlanSummary#getStartDelayMs()}, which surfaces
	 * pool-saturation symptoms.
	 */
	@NonFinal
	@Nullable
	private volatile Instant executionStartedAt;

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
				submittedAt,
				executionStartedAt,
				planState.get(),
				completedAt);
	}

	@Override
	public long version() {
		return version.get();
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
		return version.incrementAndGet();
	}

	/**
	 * Called by the engine when execution actually starts (after any pool-saturation queueing). Idempotent — the first
	 * call wins so the recorded delay isn't reset if the engine re-enters this path.
	 *
	 * @param at
	 *            wall-clock time at which execution started
	 */
	public void markExecutionStarted(Instant at) {
		if (this.executionStartedAt == null) {
			this.executionStartedAt = at;
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
		this.completedAt = at;
		bumpVersion();
	}
}
