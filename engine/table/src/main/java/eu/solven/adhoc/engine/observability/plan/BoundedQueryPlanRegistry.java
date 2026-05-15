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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import eu.solven.adhoc.query.AdhocQueryId;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory {@link IQueryPlanRegistry} with an LRU eviction policy keyed on total live node count.
 *
 * <p>
 * Properties:
 * <ul>
 * <li>Backed by a {@link LinkedHashMap} of {@link IPlanSource} entries in access-order so the LRU is implicit.</li>
 * <li>{@link #snapshot(AdhocQueryId)} delegates to {@link IPlanSource#snapshot()}, which returns a fresh immutable
 * {@link QueryPlan} per call — readers therefore never see a mid-mutation tree.</li>
 * <li>Sources that report {@link IPlanSource#isCompleted()} are eligible for LRU eviction once the registry is over
 * budget. In-flight sources are exempt.</li>
 * <li>{@link #lock(AdhocQueryId) Locked} sources sit in a separate map and are never evicted regardless of budget.
 * Their node count still contributes to {@link #totalNodeCount()} — pinning a 20k-node plan eats from the same budget
 * the LRU side competes for.</li>
 * <li>Thread-safe via {@code synchronized} blocks guarded by a private {@link #mutationLock} object. Every public
 * method acquires it before touching {@link #sources} / {@link #locked} / {@link #currentNodeCount}.</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
@Slf4j
// All public methods guard mutation/lookup on the private {@link #mutationLock} object. The codebase convention is
// to suppress PMD's AvoidSynchronizedStatement at the class level rather than per-method (see e.g. AdhocQueryMonitor).
@SuppressWarnings("PMD.AvoidSynchronizedStatement")
public class BoundedQueryPlanRegistry implements IQueryPlanRegistry {

	private final long maxTotalNodes;

	/**
	 * Access-order map holding the unlocked sources (backing impl: {@link LinkedHashMap} with access-order on). Every
	 * {@link #get} / {@link #snapshot} bumps the entry to the back; the iterator therefore starts at the LRU candidate.
	 */
	protected final Map<AdhocQueryId, IPlanSource> sources;

	/**
	 * Plans explicitly pinned via {@link #lock(AdhocQueryId)} (backing impl: {@link LinkedHashMap}). Iteration order is
	 * insertion order — irrelevant for eviction (these are never evicted), useful for reproducible
	 * {@link #getChildrenOf(AdhocQueryId)} output.
	 */
	protected final Map<AdhocQueryId, IPlanSource> locked;

	protected long currentNodeCount;

	private static final int INITIAL_MAP_CAPACITY = 64;
	private static final float MAP_LOAD_FACTOR = 0.75f;

	/**
	 * Private lock object — avoids the PMD AvoidSynchronizedAtMethodLevel pattern and shields against external callers
	 * synchronising on {@code this}. Named with a distinct identifier from the {@link #lock(AdhocQueryId)} method to
	 * keep PMD's AvoidFieldNameMatchingMethodName happy.
	 */
	private final Object mutationLock = new Object();

	public BoundedQueryPlanRegistry(long maxTotalNodes) {
		if (maxTotalNodes <= 0L) {
			throw new IllegalArgumentException("maxTotalNodes must be positive, got " + maxTotalNodes);
		}
		this.maxTotalNodes = maxTotalNodes;
		// `true` selects access-order — the LRU policy depends on this.
		this.sources = new LinkedHashMap<>(INITIAL_MAP_CAPACITY, MAP_LOAD_FACTOR, true);
		this.locked = new LinkedHashMap<>();
	}

	@Override
	public void registerSource(IPlanSource source) {
		Objects.requireNonNull(source, "source");
		AdhocQueryId id = source.getQueryId();
		synchronized (mutationLock) {
			// If the id is currently locked, replace it in place — the user expects their pin to survive a re-register.
			IPlanSource previous;
			if (locked.containsKey(id)) {
				previous = locked.put(id, source);
			} else {
				previous = sources.put(id, source);
			}
			if (previous != null) {
				currentNodeCount -= nodeCountOf(previous);
			}
			currentNodeCount += nodeCountOf(source);
			evictIfOverBudget();
		}
	}

	@Override
	public Optional<QueryPlan> get(AdhocQueryId queryId) {
		synchronized (mutationLock) {
			IPlanSource source = lookup(queryId);
			if (source == null) {
				return Optional.empty();
			}
			return Optional.of(source.snapshot());
		}
	}

	@Override
	public Optional<QueryPlan> snapshot(AdhocQueryId queryId) {
		synchronized (mutationLock) {
			IPlanSource source = lookup(queryId);
			if (source == null) {
				return Optional.empty();
			}
			return Optional.of(source.snapshot());
		}
	}

	@Override
	public List<QueryPlan> getChildrenOf(AdhocQueryId parent) {
		Objects.requireNonNull(parent, "parent");
		// Match by UUID — that's the link the engine maintains (AdhocQueryId.parentQueryId is a UUID).
		UUID parentUuid = parent.getQueryId();
		List<QueryPlan> kids = new ArrayList<>();
		synchronized (mutationLock) {
			collectChildren(sources.values(), parentUuid, kids);
			collectChildren(locked.values(), parentUuid, kids);
		}
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
		synchronized (mutationLock) {
			return sources.size() + locked.size();
		}
	}

	@Override
	public long totalNodeCount() {
		synchronized (mutationLock) {
			return currentNodeCount;
		}
	}

	@Override
	public boolean hasPlan(AdhocQueryId queryId) {
		synchronized (mutationLock) {
			return sources.containsKey(queryId) || locked.containsKey(queryId);
		}
	}

	@Override
	public boolean lock(AdhocQueryId queryId) {
		Objects.requireNonNull(queryId, "queryId");
		synchronized (mutationLock) {
			if (locked.containsKey(queryId)) {
				return false;
			}
			IPlanSource source = sources.remove(queryId);
			if (source == null) {
				return false;
			}
			locked.put(queryId, source);
			return true;
		}
	}

	@Override
	public boolean unlock(AdhocQueryId queryId) {
		Objects.requireNonNull(queryId, "queryId");
		synchronized (mutationLock) {
			IPlanSource source = locked.remove(queryId);
			if (source == null) {
				return false;
			}
			sources.put(queryId, source);
			// Now that the source is back in the LRU pool, the budget might be exceeded — give eviction a chance.
			evictIfOverBudget();
			return true;
		}
	}

	@Override
	public boolean isLocked(AdhocQueryId queryId) {
		synchronized (mutationLock) {
			return locked.containsKey(queryId);
		}
	}

	@Override
	public void publishFragment(AdhocQueryId queryId, Object anchor, QueryPlanNode subtree) {
		Objects.requireNonNull(queryId, "queryId");
		Objects.requireNonNull(anchor, "anchor");
		Objects.requireNonNull(subtree, "subtree");
		synchronized (mutationLock) {
			IPlanSource source = lookup(queryId);
			if (source instanceof LiveQueryPlanSource live) {
				live.publishFragment(anchor, subtree);
			} else if (source == null) {
				// Fragment for a queryId we don't know about — either the source was never registered (e.g. a unit
				// test driving the table engine without a CubeQueryEngine), or it's already been evicted. Either
				// way, dropping is correct: there's nothing to graft onto.
				log.debug("Dropping fragment for unknown queryId={}", queryId);
			} else {
				// A non-Live IPlanSource — fragments don't have a place to land. Defensive log; the production path
				// only registers LiveQueryPlanSource so this branch is unreachable today.
				log.warn("Cannot publish fragment onto non-Live source for queryId={}: {}",
						queryId,
						source.getClass().getSimpleName());
			}
		}
	}

	@Override
	public Optional<AdhocQueryId> findIdByUuid(UUID queryUuid) {
		Objects.requireNonNull(queryUuid, "queryUuid");
		synchronized (mutationLock) {
			for (AdhocQueryId id : sources.keySet()) {
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
	}

	/**
	 * Lookup a source by id across both the LRU pool and the locked map. Returns {@code null} when absent. Used by
	 * {@link #get(AdhocQueryId)} / {@link #snapshot(AdhocQueryId)} so they share the same dispatch logic.
	 */
	@org.jspecify.annotations.Nullable
	protected IPlanSource lookup(AdhocQueryId queryId) {
		IPlanSource source = sources.get(queryId);
		if (source != null) {
			return source;
		}
		return locked.get(queryId);
	}

	/**
	 * Walk the access-order map oldest-first, dropping completed sources until either the budget is met or no completed
	 * sources remain. In-flight sources are skipped — they will be reconsidered the next time the budget tips over.
	 */
	private void evictIfOverBudget() {
		if (currentNodeCount <= maxTotalNodes) {
			return;
		}
		Iterator<Map.Entry<AdhocQueryId, IPlanSource>> it = sources.entrySet().iterator();
		List<AdhocQueryId> evictable = new ArrayList<>();
		while (it.hasNext()) {
			Map.Entry<AdhocQueryId, IPlanSource> e = it.next();
			if (e.getValue().isCompleted()) {
				evictable.add(e.getKey());
			}
		}
		for (AdhocQueryId key : evictable) {
			if (currentNodeCount <= maxTotalNodes) {
				return;
			}
			IPlanSource removed = sources.remove(key);
			if (removed != null) {
				long count = nodeCountOf(removed);
				currentNodeCount -= count;
				log.debug("Evicted plan queryId={} nodes={} (budget={}/{})",
						key,
						count,
						currentNodeCount,
						maxTotalNodes);
			}
		}
	}

	/**
	 * Pre-projected node count for the eviction budget. Each source projects once at registration to learn its size;
	 * the cost is acceptable since register happens once per query and projection is a single dag walk.
	 */
	private static long nodeCountOf(IPlanSource source) {
		return source.snapshot().getNodeCount();
	}
}
