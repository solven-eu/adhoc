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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import eu.solven.adhoc.query.AdhocQueryId;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory {@link IQueryPlanRegistry} with an LRU eviction policy keyed on total live node count.
 *
 * <p>
 * Properties:
 * <ul>
 * <li>Backed by a {@link LinkedHashMap} of {@link IPlanSource} entries in access-order so the LRU is implicit.</li>
 * <li>{@link #snapshot(AdhocQueryId)} returns a safe-to-share {@link QueryPlan} — for {@link StaticPlanSource} it
 * deep-copies the wrapped plan; for {@link LiveQueryPlanSource} it returns the projector's fresh tree directly (the
 * projector already builds an immutable structure per call).</li>
 * <li>{@link #get(AdhocQueryId)} returns the source's raw plan — for {@link StaticPlanSource} that's the mutable
 * instance, used by the push-side {@code QueryPlanRegistryUpdater} to mutate state in place.</li>
 * <li>Sources that report {@link IPlanSource#isCompleted()} are eligible for LRU eviction once the registry is over
 * budget. In-flight sources are exempt.</li>
 * <li>{@link #lock(AdhocQueryId) Locked} sources sit in a separate map and are never evicted regardless of budget.
 * Their node count still contributes to {@link #totalNodeCount()} — pinning a 20k-node plan eats from the same budget
 * the LRU side competes for.</li>
 * <li>Thread-safe via a single {@code synchronized} on every public method.</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
@Slf4j
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

	public BoundedQueryPlanRegistry(long maxTotalNodes) {
		if (maxTotalNodes <= 0L) {
			throw new IllegalArgumentException("maxTotalNodes must be positive, got " + maxTotalNodes);
		}
		this.maxTotalNodes = maxTotalNodes;
		this.sources = new LinkedHashMap<>(64, 0.75f, true);
		this.locked = new LinkedHashMap<>();
	}

	@Override
	public synchronized void register(QueryPlan plan) {
		Objects.requireNonNull(plan, "plan");
		registerSource(new StaticPlanSource(plan));
	}

	@Override
	public synchronized void registerSource(IPlanSource source) {
		Objects.requireNonNull(source, "source");
		AdhocQueryId id = source.getQueryId();
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

	@Override
	public synchronized Optional<QueryPlan> get(AdhocQueryId queryId) {
		IPlanSource source = lookup(queryId);
		if (source == null) {
			return Optional.empty();
		}
		// Static sources expose the underlying mutable plan; live sources have nothing mutable to expose so we
		// fall back to the projection. The push-side updater always works with static sources.
		if (source instanceof StaticPlanSource s) {
			return Optional.of(s.getPlan());
		}
		return Optional.of(source.snapshot());
	}

	@Override
	public synchronized Optional<QueryPlan> snapshot(AdhocQueryId queryId) {
		IPlanSource source = lookup(queryId);
		if (source == null) {
			return Optional.empty();
		}
		if (source instanceof StaticPlanSource s) {
			// Deep-copy because the wrapped plan is mutated in place by the push-side updater. The live source's
			// own snapshot() returns a fresh immutable tree per call — no need to copy again.
			return Optional.of(deepCopy(s.getPlan()));
		}
		return Optional.of(source.snapshot());
	}

	@Override
	public synchronized List<QueryPlan> getChildrenOf(AdhocQueryId parent) {
		Objects.requireNonNull(parent, "parent");
		// Match by UUID — that's the link the engine maintains (AdhocQueryId.parentQueryId is a UUID).
		java.util.UUID parentUuid = parent.getQueryId();
		List<QueryPlan> kids = new ArrayList<>();
		collectChildren(sources.values(), parentUuid, kids);
		collectChildren(locked.values(), parentUuid, kids);
		return kids;
	}

	protected static void collectChildren(java.util.Collection<IPlanSource> srcs,
			java.util.UUID parentUuid,
			List<QueryPlan> out) {
		for (IPlanSource source : srcs) {
			QueryPlan p;
			if (source instanceof StaticPlanSource s) {
				p = deepCopy(s.getPlan());
			} else {
				p = source.snapshot();
			}
			if (parentUuid.equals(p.getParentQueryId())) {
				out.add(p);
			}
		}
	}

	@Override
	public synchronized int planCount() {
		return sources.size() + locked.size();
	}

	@Override
	public synchronized long totalNodeCount() {
		return currentNodeCount;
	}

	@Override
	public synchronized boolean hasPlan(AdhocQueryId queryId) {
		return sources.containsKey(queryId) || locked.containsKey(queryId);
	}

	@Override
	public synchronized boolean lock(AdhocQueryId queryId) {
		Objects.requireNonNull(queryId, "queryId");
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

	@Override
	public synchronized boolean unlock(AdhocQueryId queryId) {
		Objects.requireNonNull(queryId, "queryId");
		IPlanSource source = locked.remove(queryId);
		if (source == null) {
			return false;
		}
		sources.put(queryId, source);
		// Now that the source is back in the LRU pool, the budget might be exceeded — give eviction a chance.
		evictIfOverBudget();
		return true;
	}

	@Override
	public synchronized boolean isLocked(AdhocQueryId queryId) {
		return locked.containsKey(queryId);
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
		LinkedList<AdhocQueryId> evictable = new LinkedList<>();
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
	 * Pre-projected node count for the eviction budget. For static sources we read the field directly; for live sources
	 * we have to project once to learn it. The live cost is acceptable: register happens once per query and projection
	 * is a single dag walk.
	 */
	private static long nodeCountOf(IPlanSource source) {
		if (source instanceof StaticPlanSource s) {
			return s.getPlan().getNodeCount();
		}
		return source.snapshot().getNodeCount();
	}

	/**
	 * Deep-copy a plan + its node tree so the caller can read it without race against the engine. Static sources route
	 * through this; live sources don't need it. Visible for tests.
	 */
	static QueryPlan deepCopy(QueryPlan plan) {
		return plan.toBuilder().root(deepCopy(plan.getRoot())).build();
	}

	private static QueryPlanNode deepCopy(QueryPlanNode node) {
		List<QueryPlanNode> copiedChildren = node.getChildren()
				.stream()
				.map(BoundedQueryPlanRegistry::deepCopy)
				.collect(Collectors.toUnmodifiableList());
		return node.toBuilder().children(copiedChildren).details(Map.copyOf(node.getDetails())).build();
	}
}
