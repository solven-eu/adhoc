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

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import eu.solven.adhoc.query.AdhocQueryId;

/**
 * Holds the {@link QueryPlan} for every recent execution. Two consumer shapes are intended:
 *
 * <ol>
 * <li><strong>Engine</strong> — the {@code DagExplainer} variants build plans into the registry while a query is
 * executing. The plan transitions from {@link PlanState#PENDING} to {@link PlanState#RUNNING} to {@link PlanState#DONE}
 * / {@link PlanState#FAILED} as nodes complete.</li>
 * <li><strong>UI / log renderer</strong> — read-side. The log renderer dumps the plan on completion; the Live View
 * polls {@link #snapshot(AdhocQueryId)} every few hundred ms while the plan is RUNNING and stops once it reaches a
 * terminal state. {@link #snapshot} returns a deep copy so mutation on the engine side is never observed mid-flight by
 * readers.</li>
 * </ol>
 *
 * <p>
 * Composite-cube fan-outs register one plan per execution — root + N sub-cubes, with each sub-cube's
 * {@link QueryPlan#getParentQueryId() parentQueryId} pointing back at the root. UI navigation walks
 * {@link #getChildrenOf(AdhocQueryId)} to traverse the hierarchy.
 *
 * <p>
 * Memory budget: the registry caps its size by total live <strong>node</strong> count (not plan count) so a stream of
 * small plans accumulates freely while a single 20k-node plan eats its fair share of the budget. Eviction is LRU on
 * completed plans; in-flight plans (state != DONE / FAILED) are never evicted.
 *
 * @author Benoit Lacelle
 */
// TODO Roadmap: rendering helpers. A textual plan dump (current `DagExplainer` style — fits in a log) and a
// Mermaid `graph TD` form (fits in a docs page or modal). Mermaid won't render the 20k-node case well; for very
// large plans, fall back to text or a "show top-N hottest steps" digest.
//
// TODO Roadmap: HTTP surface. `GET /api/v1/cubes/queries/{id}/plan/summary` returns the cheap status;
// `.../plan/snapshot`
// returns the full plan tree; `.../plan/mermaid` and `.../plan/text` (future) render. Same path family enables the
// query-history endpoint to enrich entries with a `hasPlan` flag via {@link #hasPlan(AdhocQueryId)}.
public interface IQueryPlanRegistry {

	/**
	 * Register a freshly-built plan via the push-side path (the {@code QueryPlanRegistryUpdater} pattern). Idempotent
	 * on {@code plan.queryId} — re-registering replaces the existing entry, which is the normal path when the engine
	 * transitions a plan from PENDING to RUNNING (or updates after a node completes).
	 *
	 * <p>
	 * Equivalent to wrapping {@code plan} in a {@link StaticPlanSource} and calling
	 * {@link #registerSource(IPlanSource)} — convenience overload for callers that already have a complete
	 * {@link QueryPlan}.
	 *
	 * @param plan
	 *            the plan; must not be {@code null}
	 */
	@Deprecated(since = "Push will be removed")
	void register(QueryPlan plan);

	/**
	 * Register a {@link IPlanSource} — the pull-side path used by {@link LiveQueryPlanSource}, which projects a
	 * {@code QueryStepsDag} on demand. Idempotent on the source's {@link IPlanSource#getQueryId() queryId}.
	 *
	 * @param source
	 *            the source; must not be {@code null}
	 */
	void registerSource(IPlanSource source);

	/**
	 * Direct lookup of the (possibly still mutating) plan by id. Prefer {@link #snapshot(AdhocQueryId)} for read paths
	 * that may run concurrently with the engine.
	 *
	 * @param queryId
	 *            the {@link AdhocQueryId} to look up
	 * @return the plan if present, empty otherwise (or if evicted by the size cap)
	 */
	Optional<QueryPlan> get(AdhocQueryId queryId);

	/**
	 * Deep-copy snapshot — safe for read paths that may run concurrently with engine-side mutation. UI / poll handlers
	 * should always go through this.
	 *
	 * @param queryId
	 *            the {@link AdhocQueryId} to look up
	 * @return a deep-copied plan that will not be touched by further engine mutations, or empty if absent
	 */
	Optional<QueryPlan> snapshot(AdhocQueryId queryId);

	/**
	 * Return every registered plan whose {@link QueryPlan#getParentQueryId() parentQueryId} equals {@code parent}.
	 * Order is insertion order. Empty list when no children are registered (which is the normal case for non-composite
	 * queries).
	 *
	 * @param parent
	 *            the parent {@link AdhocQueryId}
	 * @return the child plans (deep-copied — same safety guarantee as {@link #snapshot(AdhocQueryId)})
	 */
	List<QueryPlan> getChildrenOf(AdhocQueryId parent);

	/**
	 * @return the current total of plans stored in the registry. Includes in-flight and completed-but-not-yet-evicted
	 *         plans.
	 */
	int planCount();

	/**
	 * @return the sum of {@link QueryPlan#getNodeCount()} over every plan in the registry — the dimension the eviction
	 *         policy operates on.
	 */
	long totalNodeCount();

	/**
	 * Cheap presence check — true when the registry currently holds an entry for {@code queryId} (locked OR unlocked).
	 * Used by query-history endpoints to decide whether to render a "view plan" affordance next to each historical
	 * entry without paying for a full snapshot.
	 *
	 * @param queryId
	 *            the {@link AdhocQueryId} to test
	 * @return true if a plan can be served for this id
	 */
	default boolean hasPlan(AdhocQueryId queryId) {
		return get(queryId).isPresent();
	}

	/**
	 * Pin {@code queryId} so the registry's LRU eviction never drops it. Implementations move the entry to a separate
	 * "locked" map so the normal eviction loop only walks the LRU side. Idempotent — locking an already-locked id is a
	 * no-op; locking an unknown id is a no-op.
	 *
	 * <p>
	 * Use case: the user opened a Live View on a long-running query and explicitly clicked "Pin" — the plan must
	 * survive whatever burst of other queries the system runs in the meantime. {@link #unlock(AdhocQueryId)} moves the
	 * entry back into the LRU pool where it can be evicted normally.
	 *
	 * @param queryId
	 *            the {@link AdhocQueryId} to pin
	 * @return true if the registry held the id and pinning had an effect (state change); false otherwise
	 */
	default boolean lock(AdhocQueryId queryId) {
		return false;
	}

	/**
	 * Counterpart to {@link #lock(AdhocQueryId)}. Moves the entry from the locked map back to the LRU pool. Idempotent.
	 *
	 * @param queryId
	 *            the {@link AdhocQueryId} to release
	 * @return true if the registry held the id as locked and the unlock had an effect; false otherwise
	 */
	default boolean unlock(AdhocQueryId queryId) {
		return false;
	}

	/**
	 * @param queryId
	 *            the {@link AdhocQueryId} to test
	 * @return true when the entry is currently in the locked map
	 */
	default boolean isLocked(AdhocQueryId queryId) {
		return false;
	}

	/**
	 * UUID-based lookup helper for HTTP endpoints, which carry only the {@link UUID} part of an {@link AdhocQueryId} in
	 * their URL. Scans the registered ids; O(N) where N is the registry size — fine for the bounded sizes the registry
	 * holds (hundreds, not millions). The default implementation returns empty; backing impls override.
	 *
	 * @param queryUuid
	 *            the UUID to match against {@link AdhocQueryId#getQueryId()}
	 * @return the matching {@link AdhocQueryId}, or empty when no plan is registered for that UUID
	 */
	default Optional<AdhocQueryId> findIdByUuid(UUID queryUuid) {
		return Optional.empty();
	}
}
