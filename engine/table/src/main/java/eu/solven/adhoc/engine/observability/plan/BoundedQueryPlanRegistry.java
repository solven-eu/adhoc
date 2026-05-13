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
 * <li>Backed by a {@link LinkedHashMap} in access-order so the LRU is implicit.</li>
 * <li>{@link #snapshot(AdhocQueryId)} returns a deep copy via the same builder pattern used to construct plans — engine
 * mutation cannot torpedo a concurrent reader.</li>
 * <li>In-flight plans (state != DONE / FAILED) are exempt from eviction. They will be reconsidered the next time a
 * register / get touches the budget.</li>
 * <li>Thread-safe via a single {@code synchronized} on every public method. Contention should be low: the engine writes
 * per-step, the UI reads at &lt;10Hz; we can revisit if profiling shows the lock is hot.</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class BoundedQueryPlanRegistry implements IQueryPlanRegistry {

	/**
	 * Soft cap on the total number of {@link QueryPlanNode} stored across every registered plan. When exceeded,
	 * completed plans are evicted oldest-first until the cap is met or no completed plans remain.
	 */
	private final long maxTotalNodes;

	/**
	 * Access-order LinkedHashMap: every {@link #get} / {@link #snapshot} bumps the entry to the back, making the
	 * iterator's first element the LRU candidate.
	 */
	private final LinkedHashMap<AdhocQueryId, QueryPlan> plans;

	private long currentNodeCount;

	/**
	 * @param maxTotalNodes
	 *            upper bound on stored {@link QueryPlanNode} count. Must be positive.
	 */
	public BoundedQueryPlanRegistry(long maxTotalNodes) {
		if (maxTotalNodes <= 0L) {
			throw new IllegalArgumentException("maxTotalNodes must be positive, got " + maxTotalNodes);
		}
		this.maxTotalNodes = maxTotalNodes;
		// `accessOrder=true` so iteration starts at the LRU entry. Initial capacity small; resize is cheap.
		this.plans = new LinkedHashMap<>(64, 0.75f, true);
	}

	@Override
	public synchronized void register(QueryPlan plan) {
		Objects.requireNonNull(plan, "plan");
		QueryPlan previous = plans.put(plan.getQueryId(), plan);
		if (previous != null) {
			currentNodeCount -= previous.getNodeCount();
		}
		currentNodeCount += plan.getNodeCount();
		evictIfOverBudget();
	}

	@Override
	public synchronized Optional<QueryPlan> get(AdhocQueryId queryId) {
		// LinkedHashMap#get bumps access-order — desirable here: a query the UI is actively polling stays warm.
		return Optional.ofNullable(plans.get(queryId));
	}

	@Override
	public synchronized Optional<QueryPlan> snapshot(AdhocQueryId queryId) {
		return get(queryId).map(BoundedQueryPlanRegistry::deepCopy);
	}

	@Override
	public synchronized List<QueryPlan> getChildrenOf(AdhocQueryId parent) {
		Objects.requireNonNull(parent, "parent");
		List<QueryPlan> kids = new ArrayList<>();
		for (QueryPlan p : plans.values()) {
			if (parent.equals(p.getParentQueryId())) {
				kids.add(deepCopy(p));
			}
		}
		return kids;
	}

	@Override
	public synchronized int planCount() {
		return plans.size();
	}

	@Override
	public synchronized long totalNodeCount() {
		return currentNodeCount;
	}

	/**
	 * Walk the access-order map oldest-first, dropping completed plans until either the budget is met or no completed
	 * plans remain. In-flight plans (PENDING / RUNNING) are skipped — they will be reconsidered the next time the
	 * budget tips over.
	 */
	private void evictIfOverBudget() {
		if (currentNodeCount <= maxTotalNodes) {
			return;
		}
		// Snapshot keys to avoid concurrent modification while iterating + removing.
		Iterator<Map.Entry<AdhocQueryId, QueryPlan>> it = plans.entrySet().iterator();
		LinkedList<AdhocQueryId> evictable = new LinkedList<>();
		while (it.hasNext()) {
			Map.Entry<AdhocQueryId, QueryPlan> e = it.next();
			QueryPlan p = e.getValue();
			if (p.getState() == PlanState.DONE || p.getState() == PlanState.FAILED) {
				evictable.add(e.getKey());
			}
		}
		for (AdhocQueryId key : evictable) {
			if (currentNodeCount <= maxTotalNodes) {
				return;
			}
			QueryPlan removed = plans.remove(key);
			if (removed != null) {
				currentNodeCount -= removed.getNodeCount();
				log.debug("Evicted plan queryId={} nodes={} (budget={}/{})",
						key,
						removed.getNodeCount(),
						currentNodeCount,
						maxTotalNodes);
			}
		}
	}

	/**
	 * Deep-copy a plan + its node tree so the caller can read it without race against the engine. Uses the Lombok
	 * builders' {@code toBuilder()} so we get correct field-by-field copies even as new fields are added.
	 */
	static QueryPlan deepCopy(QueryPlan plan) {
		return plan.toBuilder().root(deepCopy(plan.getRoot())).build();
	}

	private static QueryPlanNode deepCopy(QueryPlanNode node) {
		List<QueryPlanNode> copiedChildren = node.getChildren()
				.stream()
				.map(BoundedQueryPlanRegistry::deepCopy)
				.collect(Collectors.toUnmodifiableList());
		// stats is immutable (Lombok @Value); reusing the reference is safe.
		// details map: shallow-copy keeps semantics if the engine later mutates the original.
		return node.toBuilder().children(copiedChildren).details(Map.copyOf(node.getDetails())).build();
	}
}
