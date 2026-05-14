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
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.AdhocQueryId;

public class TestBoundedQueryPlanRegistry {

	/**
	 * Minimal {@link IPlanSource} that returns a pre-built {@link QueryPlan} verbatim. Used by tests that want to
	 * exercise the registry's bookkeeping without standing up a full {@code LiveQueryPlanSource} (which needs a real
	 * {@code QueryStepsDag}).
	 */
	private static final class FixedPlanSource implements IPlanSource {
		private final QueryPlan plan;

		FixedPlanSource(QueryPlan plan) {
			this.plan = plan;
		}

		@Override
		public AdhocQueryId getQueryId() {
			return plan.getQueryId();
		}

		@Override
		public QueryPlan snapshot() {
			return plan;
		}

		@Override
		public boolean isCompleted() {
			PlanState s = plan.getState();
			return s == PlanState.DONE || s == PlanState.FAILED;
		}
	}

	private static void registerPlan(BoundedQueryPlanRegistry registry, QueryPlan plan) {
		registry.registerSource(new FixedPlanSource(plan));
	}

	private static AdhocQueryId newId() {
		return AdhocQueryId.builder().cube("test-cube").build();
	}

	/** Minimal plan factory. {@code nodeCount} is reported as advertised, regardless of the actual tree size. */
	private static QueryPlan newPlan(AdhocQueryId id, AdhocQueryId parent, PlanState state, long nodeCount) {
		QueryPlanNode root = QueryPlanNode.builder()
				.subject("root-of-" + id.getQueryIndex())
				.operator(NodeOperator.CUBE_STEP)
				.label("root")
				.state(state == PlanState.PENDING ? NodeState.PENDING : NodeState.DONE)
				.build();
		return QueryPlan.builder()
				.queryId(id)
				.parentQueryId(parent == null ? null : parent.getQueryId())
				.cubeName("test-cube")
				.state(state)
				.submittedAt(Instant.now())
				.completedAt(state == PlanState.DONE || state == PlanState.FAILED ? Instant.now() : null)
				.root(root)
				.nodeCount(nodeCount)
				.build();
	}

	@Test
	public void testRegisterAndGet() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId id = newId();
		QueryPlan plan = newPlan(id, null, PlanState.PENDING, 5);

		registerPlan(registry, plan);

		Assertions.assertThat(registry.get(id)).contains(plan);
		Assertions.assertThat(registry.planCount()).isEqualTo(1);
		Assertions.assertThat(registry.totalNodeCount()).isEqualTo(5);
	}

	@Test
	public void testGetReturnsEmptyForUnknownId() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		Assertions.assertThat(registry.get(newId())).isEmpty();
		Assertions.assertThat(registry.snapshot(newId())).isEmpty();
	}

	@Test
	public void testReRegisterReplacesAndAdjustsNodeCount() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId id = newId();

		registerPlan(registry, newPlan(id, null, PlanState.PENDING, 5));
		Assertions.assertThat(registry.totalNodeCount()).isEqualTo(5);

		// Same id, different node count — simulates the PENDING-to-RUNNING transition that grows the tree.
		registerPlan(registry, newPlan(id, null, PlanState.RUNNING, 12));
		Assertions.assertThat(registry.planCount()).isEqualTo(1);
		Assertions.assertThat(registry.totalNodeCount()).isEqualTo(12);
	}

	@Test
	public void testSnapshotDelegatesToSource() {
		// The registry no longer deep-copies — it trusts each {@link IPlanSource} to produce a safe-to-share plan on
		// every {@code snapshot()} call. {@link LiveQueryPlanSource} does so by re-projecting the dag; a test fixture
		// can choose to return the same instance.
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId id = newId();
		QueryPlan plan = newPlan(id, null, PlanState.DONE, 1);
		registerPlan(registry, plan);

		QueryPlan snapshot = registry.snapshot(id).orElseThrow();
		Assertions.assertThat(snapshot).isEqualTo(plan);
	}

	@Test
	public void testGetChildrenOfFiltersByParent() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId parent = newId();
		AdhocQueryId child1 = newId();
		AdhocQueryId child2 = newId();
		AdhocQueryId unrelated = newId();

		registerPlan(registry, newPlan(parent, null, PlanState.RUNNING, 1));
		registerPlan(registry, newPlan(child1, parent, PlanState.RUNNING, 1));
		registerPlan(registry, newPlan(child2, parent, PlanState.RUNNING, 1));
		registerPlan(registry, newPlan(unrelated, null, PlanState.RUNNING, 1));

		List<QueryPlan> children = registry.getChildrenOf(parent);
		Assertions.assertThat(children).extracting(QueryPlan::getQueryId).containsExactlyInAnyOrder(child1, child2);
	}

	@Test
	public void testGetChildrenOfReturnsEmptyForLeafQuery() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId id = newId();
		registerPlan(registry, newPlan(id, null, PlanState.DONE, 1));

		Assertions.assertThat(registry.getChildrenOf(id)).isEmpty();
	}

	@Test
	public void testEvictionDropsOldestCompletedWhenOverBudget() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10);

		// 3 completed plans, 4 nodes each → 12 > 10 budget; oldest should be evicted on the third register.
		AdhocQueryId a = newId();
		AdhocQueryId b = newId();
		AdhocQueryId c = newId();
		registerPlan(registry, newPlan(a, null, PlanState.DONE, 4));
		registerPlan(registry, newPlan(b, null, PlanState.DONE, 4));
		registerPlan(registry, newPlan(c, null, PlanState.DONE, 4));

		Assertions.assertThat(registry.totalNodeCount()).isLessThanOrEqualTo(10);
		Assertions.assertThat(registry.get(a)).as("oldest should be evicted").isEmpty();
		Assertions.assertThat(registry.get(b)).isPresent();
		Assertions.assertThat(registry.get(c)).isPresent();
	}

	@Test
	public void testInFlightPlansAreNeverEvicted() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10);

		// An in-flight plan + a completed one + a bigger plan that should force eviction. The in-flight plan must
		// survive even when the budget is exceeded.
		AdhocQueryId inFlight = newId();
		AdhocQueryId completed = newId();
		AdhocQueryId fresh = newId();

		registerPlan(registry, newPlan(inFlight, null, PlanState.RUNNING, 8));
		registerPlan(registry, newPlan(completed, null, PlanState.DONE, 4));
		// Re-registering inFlight as still-RUNNING (the engine's normal update path) does not change its state.
		registerPlan(registry, newPlan(fresh, null, PlanState.DONE, 6));

		Assertions.assertThat(registry.get(inFlight)).as("in-flight plan must survive eviction").isPresent();
		// Either `completed` or `fresh` must have been evicted to satisfy the budget.
		Assertions.assertThat(registry.totalNodeCount()).isLessThanOrEqualTo(10L + 8L);
	}

	@Test
	public void testEvictionStopsOnceUnderBudget() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10);

		AdhocQueryId a = newId();
		AdhocQueryId b = newId();
		AdhocQueryId c = newId();
		AdhocQueryId d = newId();
		registerPlan(registry, newPlan(a, null, PlanState.DONE, 4));
		registerPlan(registry, newPlan(b, null, PlanState.DONE, 4));
		registerPlan(registry, newPlan(c, null, PlanState.DONE, 1));
		// d pushes total to 12 (>10). Eviction must drop `a` (4 nodes) → total 8 (<=10). It must NOT continue evicting
		// `b` since we are already under budget.
		registerPlan(registry, newPlan(d, null, PlanState.DONE, 3));

		Assertions.assertThat(registry.get(a)).isEmpty();
		Assertions.assertThat(registry.get(b)).isPresent();
		Assertions.assertThat(registry.get(c)).isPresent();
		Assertions.assertThat(registry.get(d)).isPresent();
		Assertions.assertThat(registry.totalNodeCount()).isEqualTo(8);
	}

	@Test
	public void testLockMovesEntryAndProtectsFromEviction() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10);
		AdhocQueryId a = newId();
		AdhocQueryId b = newId();
		AdhocQueryId c = newId();
		registerPlan(registry, newPlan(a, null, PlanState.DONE, 4));
		registerPlan(registry, newPlan(b, null, PlanState.DONE, 4));

		// Pin `a`. It would otherwise be the LRU eviction candidate.
		Assertions.assertThat(registry.lock(a)).isTrue();
		Assertions.assertThat(registry.isLocked(a)).isTrue();
		Assertions.assertThat(registry.isLocked(b)).isFalse();

		// Register a third plan pushing us over budget (12 > 10). Eviction must skip `a` (locked) and drop `b`.
		registerPlan(registry, newPlan(c, null, PlanState.DONE, 4));
		Assertions.assertThat(registry.get(a)).isPresent();
		Assertions.assertThat(registry.get(b)).isEmpty();
		Assertions.assertThat(registry.get(c)).isPresent();
	}

	@Test
	public void testLockIsIdempotentAndUnknownIsNoop() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10);
		AdhocQueryId a = newId();
		registerPlan(registry, newPlan(a, null, PlanState.DONE, 1));

		Assertions.assertThat(registry.lock(a)).isTrue();
		// Already locked → no state change.
		Assertions.assertThat(registry.lock(a)).isFalse();
		Assertions.assertThat(registry.isLocked(a)).isTrue();

		// Unknown id → no-op.
		AdhocQueryId ghost = newId();
		Assertions.assertThat(registry.lock(ghost)).isFalse();
		Assertions.assertThat(registry.isLocked(ghost)).isFalse();
	}

	@Test
	public void testUnlockRestoresLruEligibility() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10);
		AdhocQueryId a = newId();
		registerPlan(registry, newPlan(a, null, PlanState.DONE, 4));
		Assertions.assertThat(registry.lock(a)).isTrue();
		Assertions.assertThat(registry.unlock(a)).isTrue();
		Assertions.assertThat(registry.isLocked(a)).isFalse();
		// Idempotent: unlocking again is a no-op.
		Assertions.assertThat(registry.unlock(a)).isFalse();
		Assertions.assertThat(registry.get(a)).isPresent();
	}

	@Test
	public void testHasPlanCoversBothMaps() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10);
		AdhocQueryId a = newId();
		AdhocQueryId b = newId();
		registerPlan(registry, newPlan(a, null, PlanState.DONE, 1));
		registerPlan(registry, newPlan(b, null, PlanState.DONE, 1));
		registry.lock(a);

		Assertions.assertThat(registry.hasPlan(a)).isTrue(); // in `locked`
		Assertions.assertThat(registry.hasPlan(b)).isTrue(); // in `sources`
		Assertions.assertThat(registry.hasPlan(newId())).isFalse();
	}

	@Test
	public void testPlanCountSumsBothMaps() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10);
		AdhocQueryId a = newId();
		AdhocQueryId b = newId();
		registerPlan(registry, newPlan(a, null, PlanState.DONE, 1));
		registerPlan(registry, newPlan(b, null, PlanState.DONE, 1));
		registry.lock(a);

		Assertions.assertThat(registry.planCount()).isEqualTo(2);
	}

	@Test
	public void testGetChildrenOfWalksBothMaps() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(20);
		AdhocQueryId parent = newId();
		AdhocQueryId childLocked = newId();
		AdhocQueryId childUnlocked = newId();
		registerPlan(registry, newPlan(parent, null, PlanState.DONE, 1));
		registerPlan(registry, newPlan(childLocked, parent, PlanState.DONE, 1));
		registerPlan(registry, newPlan(childUnlocked, parent, PlanState.DONE, 1));
		registry.lock(childLocked);

		List<QueryPlan> kids = registry.getChildrenOf(parent);
		Assertions.assertThat(kids).hasSize(2);
	}

	@Test
	public void testNegativeOrZeroBudgetIsRejected() {
		Assertions.assertThatThrownBy(() -> new BoundedQueryPlanRegistry(0))
				.isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThatThrownBy(() -> new BoundedQueryPlanRegistry(-1))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
