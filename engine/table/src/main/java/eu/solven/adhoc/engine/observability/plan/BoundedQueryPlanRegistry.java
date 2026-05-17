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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.query.AdhocQueryId;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory {@link IQueryPlanRegistry} with an LRU+weight eviction policy keyed on total live node count.
 *
 * <p>
 * Properties:
 * <ul>
 * <li>Unlocked sources sit in a Guava {@link Cache} configured with {@code maximumWeight(maxTotalNodes)} and a weigher
 * returning {@link IPlanSource#snapshot()}'s {@code nodeCount}. Eviction is therefore automatic, LRU-driven, and
 * lock-free; the explicit {@code synchronized}/{@code mutationLock} the previous impl carried is gone.</li>
 * <li>{@link #snapshot(AdhocQueryId)} delegates to {@link IPlanSource#snapshot()}, which returns a fresh immutable
 * {@link QueryPlan} per call — readers therefore never see a mid-mutation tree.</li>
 * <li>{@link #lock(AdhocQueryId) Locked} sources live in a separate {@link ConcurrentHashMap} and are never evicted
 * regardless of budget. Their node count still contributes to {@link #totalNodeCount()} — pinning a 20k-node plan eats
 * from the same budget the LRU side competes for.</li>
 * <li>Thread-safe via the underlying {@link Cache} (concurrent) + {@link ConcurrentMap} primitives + an
 * {@link AtomicLong} for the budget counter. There is no external lock.</li>
 * </ul>
 *
 * <p>
 * Note on the "in-flight protection" semantics that the previous impl carried: Guava's {@link Cache} does plain LRU
 * eviction and does not consult {@link IPlanSource#isCompleted()}. In production this is fine — status pollers (UI Live
 * View, programmatic monitors) keep in-flight plans warm, so they stay out of the LRU tail. A plan that is in-flight
 * AND has no observer can be evicted under pressure; the engine itself does not depend on the registry to complete the
 * query, so the only visible effect is monitors losing access to the plan.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class BoundedQueryPlanRegistry implements IQueryPlanRegistry {

	/**
	 * Total node count across both maps. Maintained explicitly (additions on register / lock-restore, decrements via
	 * the cache's removal listener). Used by {@link #totalNodeCount()} without needing to walk and project every plan.
	 */
	private final AtomicLong nodeBudgetCounter = new AtomicLong();

	/**
	 * Unlocked-and-eligible-for-eviction sources. Guava {@link Cache} with {@code maximumWeight} = {@code
	 * maxTotalNodes} and a node-count weigher; eviction is LRU under weight pressure, no external lock needed.
	 *
	 * <p>
	 * The {@link Cache#asMap()} view is used for direct put/remove operations whose return-value semantics we rely on
	 * (e.g. {@code lock} reads the removed source). {@link Cache#put} (no-arg) is equivalent.
	 */
	protected final Cache<AdhocQueryId, IPlanSource> sources;

	/**
	 * Plans explicitly pinned via {@link #lock(AdhocQueryId)}. {@link ConcurrentHashMap} so mutation/lookup is
	 * lock-free.
	 */
	protected final ConcurrentMap<AdhocQueryId, IPlanSource> locked = new ConcurrentHashMap<>();

	public BoundedQueryPlanRegistry(long maxTotalNodes) {
		if (maxTotalNodes <= 0L) {
			throw new IllegalArgumentException("maxTotalNodes must be positive, got " + maxTotalNodes);
		}
		// `weigher` is called once on put (Guava memoises the weight per entry). nodeCountOf is cheap when the
		// source is a FixedPlanSource (tests) and a single dag walk for LiveQueryPlanSource (production).
		this.sources = CacheBuilder.newBuilder()
				.maximumWeight(maxTotalNodes)
				// Weigher returns int; saturate to MAX_VALUE for outlier plans rather than overflowing.
				.<AdhocQueryId, IPlanSource>weigher((id, src) -> Ints.saturatedCast(nodeCountOf(src)))
				// Decrement the budget counter on every removal — eviction (cause=SIZE), explicit (lock/unlock or
				// re-register replace, cause=EXPLICIT/REPLACED), or expired (we don't use TTL). Manual ops (lock,
				// unlock, register) re-add to the counter when they place the source elsewhere.
				.removalListener((com.google.common.cache.RemovalNotification<AdhocQueryId, IPlanSource> notif) -> {
					IPlanSource removed = notif.getValue();
					if (removed != null) {
						long count = nodeCountOf(removed);
						nodeBudgetCounter.addAndGet(-count);
						if (notif.wasEvicted()) {
							log.debug("Evicted plan queryId={} nodes={}", notif.getKey(), count);
						}
					}
				})
				.build();
	}

	@Override
	public void registerSource(IPlanSource source) {
		Objects.requireNonNull(source, "source");
		AdhocQueryId id = source.getQueryId();
		long newCount = nodeCountOf(source);

		// If the id is currently locked, replace in place — the user expects their pin to survive a re-register.
		// `replace` is atomic on ConcurrentHashMap; returns the previous value (or null if absent).
		IPlanSource previouslyLocked = locked.replace(id, source);
		if (previouslyLocked != null) {
			nodeBudgetCounter.addAndGet(newCount - nodeCountOf(previouslyLocked));
			return;
		}

		// Not locked — put in the LRU cache. Two paths:
		// - Fresh id: no removalListener fires; we add `newCount` manually.
		// - Replacement: removalListener fires synchronously with cause=REPLACED, decrementing the old count; we
		// then add `newCount` manually for the new entry.
		sources.put(id, source);
		nodeBudgetCounter.addAndGet(newCount);
	}

	@Override
	public Optional<QueryPlan> get(AdhocQueryId queryId) {
		return snapshot(queryId);
	}

	@Override
	public Optional<QueryPlan> snapshot(AdhocQueryId queryId) {
		IPlanSource source = lookup(queryId);
		if (source == null) {
			return Optional.empty();
		}
		return Optional.of(source.snapshot());
	}

	@Override
	public List<QueryPlan> getChildrenOf(AdhocQueryId parent) {
		Objects.requireNonNull(parent, "parent");
		// Match by UUID — that's the link the engine maintains (AdhocQueryId.parentQueryId is a UUID).
		UUID parentUuid = parent.getQueryId();
		List<QueryPlan> kids = new ArrayList<>();
		collectChildren(sources.asMap().values(), parentUuid, kids);
		collectChildren(locked.values(), parentUuid, kids);
		return kids;
	}

	protected static void collectChildren(java.util.Collection<IPlanSource> srcs,
			UUID parentUuid,
			List<QueryPlan> out) {
		for (IPlanSource source : srcs) {
			QueryPlan p = source.snapshot();
			if (parentUuid.equals(p.getParentQueryId())) {
				out.add(p);
			}
		}
	}

	@Override
	public int planCount() {
		// `sources.size()` is approximate under concurrent mutation but matches the Guava contract; the test
		// assertions are about quiesced state where the value is exact.
		return Ints.saturatedCast(sources.size()) + locked.size();
	}

	@Override
	public long totalNodeCount() {
		return nodeBudgetCounter.get();
	}

	@Override
	public boolean hasPlan(AdhocQueryId queryId) {
		return sources.asMap().containsKey(queryId) || locked.containsKey(queryId);
	}

	@Override
	public boolean lock(AdhocQueryId queryId) {
		Objects.requireNonNull(queryId, "queryId");
		if (locked.containsKey(queryId)) {
			return false;
		}
		// `remove` fires the removalListener synchronously, which decrements `nodeBudgetCounter`. We re-add it below
		// when placing the source in `locked` — net zero.
		IPlanSource source = sources.asMap().remove(queryId);
		if (source == null) {
			return false;
		}
		locked.put(queryId, source);
		nodeBudgetCounter.addAndGet(nodeCountOf(source));
		return true;
	}

	@Override
	public boolean unlock(AdhocQueryId queryId) {
		Objects.requireNonNull(queryId, "queryId");
		IPlanSource source = locked.remove(queryId);
		if (source == null) {
			return false;
		}
		// Manual decrement for the locked side (no listener for ConcurrentHashMap.remove), then a re-add below
		// after `sources.put` — net zero, but the symmetric explicit ops make the bookkeeping easy to follow.
		nodeBudgetCounter.addAndGet(-nodeCountOf(source));
		sources.put(queryId, source);
		nodeBudgetCounter.addAndGet(nodeCountOf(source));
		return true;
	}

	@Override
	public boolean isLocked(AdhocQueryId queryId) {
		return locked.containsKey(queryId);
	}

	@Override
	public void publishFragment(AdhocQueryId queryId, Object anchor, QueryPlanNode subtree) {
		Objects.requireNonNull(queryId, "queryId");
		Objects.requireNonNull(anchor, "anchor");
		Objects.requireNonNull(subtree, "subtree");
		IPlanSource source = lookup(queryId);
		if (source instanceof LiveQueryPlanSource live) {
			live.publishFragment(anchor, subtree);
		} else if (source == null) {
			// Fragment for a queryId we don't know about — either the source was never registered (e.g. a unit
			// test driving the table engine without a CubeQueryEngine), or it's already been evicted. Either way,
			// dropping is correct: there's nothing to graft onto.
			log.debug("Dropping fragment for unknown queryId={}", queryId);
		} else {
			// A non-Live IPlanSource — fragments don't have a place to land. Defensive log; the production path
			// only registers LiveQueryPlanSource so this branch is unreachable today.
			log.warn("Cannot publish fragment onto non-Live source for queryId={}: {}",
					queryId,
					source.getClass().getSimpleName());
		}
	}

	@Override
	public Optional<AdhocQueryId> findIdByUuid(UUID queryUuid) {
		Objects.requireNonNull(queryUuid, "queryUuid");
		for (AdhocQueryId id : sources.asMap().keySet()) {
			if (queryUuid.equals(id.getQueryId())) {
				return Optional.of(id);
			}
		}
		for (AdhocQueryId id : locked.keySet()) {
			if (queryUuid.equals(id.getQueryId())) {
				return Optional.of(id);
			}
		}
		return Optional.empty();
	}

	/**
	 * Lookup a source by id across both the LRU cache and the locked map. Returns {@code null} when absent. Used by
	 * {@link #get(AdhocQueryId)} / {@link #snapshot(AdhocQueryId)} so they share the same dispatch logic.
	 */
	@org.jspecify.annotations.Nullable
	protected IPlanSource lookup(AdhocQueryId queryId) {
		IPlanSource source = sources.getIfPresent(queryId);
		if (source != null) {
			return source;
		}
		return locked.get(queryId);
	}

	/**
	 * Pre-projected node count for the eviction budget. Each source projects on demand to learn its size; the cost is
	 * acceptable since the projector is a single dag walk and the result is cached by Guava's weigher per entry.
	 */
	private static long nodeCountOf(IPlanSource source) {
		return source.snapshot().getNodeCount();
	}
}
