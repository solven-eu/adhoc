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
				.parentQueryId(parent)
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

		registry.register(plan);

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

		registry.register(newPlan(id, null, PlanState.PENDING, 5));
		Assertions.assertThat(registry.totalNodeCount()).isEqualTo(5);

		// Same id, different node count — simulates the PENDING-to-RUNNING transition that grows the tree.
		registry.register(newPlan(id, null, PlanState.RUNNING, 12));
		Assertions.assertThat(registry.planCount()).isEqualTo(1);
		Assertions.assertThat(registry.totalNodeCount()).isEqualTo(12);
	}

	@Test
	public void testSnapshotReturnsDeepCopy() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId id = newId();
		QueryPlan plan = newPlan(id, null, PlanState.DONE, 1);
		registry.register(plan);

		QueryPlan snapshot = registry.snapshot(id).orElseThrow();

		// The snapshot equals the registered plan (field-wise) but is not the same instance — engine mutations on the
		// real plan must not leak into the reader's view.
		Assertions.assertThat(snapshot).isEqualTo(plan);
		Assertions.assertThat(snapshot.getRoot()).isNotSameAs(plan.getRoot());
	}

	@Test
	public void testGetChildrenOfFiltersByParent() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId parent = newId();
		AdhocQueryId child1 = newId();
		AdhocQueryId child2 = newId();
		AdhocQueryId unrelated = newId();

		registry.register(newPlan(parent, null, PlanState.RUNNING, 1));
		registry.register(newPlan(child1, parent, PlanState.RUNNING, 1));
		registry.register(newPlan(child2, parent, PlanState.RUNNING, 1));
		registry.register(newPlan(unrelated, null, PlanState.RUNNING, 1));

		List<QueryPlan> children = registry.getChildrenOf(parent);
		Assertions.assertThat(children).extracting(QueryPlan::getQueryId).containsExactlyInAnyOrder(child1, child2);
	}

	@Test
	public void testGetChildrenOfReturnsEmptyForLeafQuery() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId id = newId();
		registry.register(newPlan(id, null, PlanState.DONE, 1));

		Assertions.assertThat(registry.getChildrenOf(id)).isEmpty();
	}

	@Test
	public void testEvictionDropsOldestCompletedWhenOverBudget() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10);

		// 3 completed plans, 4 nodes each → 12 > 10 budget; oldest should be evicted on the third register.
		AdhocQueryId a = newId();
		AdhocQueryId b = newId();
		AdhocQueryId c = newId();
		registry.register(newPlan(a, null, PlanState.DONE, 4));
		registry.register(newPlan(b, null, PlanState.DONE, 4));
		registry.register(newPlan(c, null, PlanState.DONE, 4));

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

		registry.register(newPlan(inFlight, null, PlanState.RUNNING, 8));
		registry.register(newPlan(completed, null, PlanState.DONE, 4));
		// Re-registering inFlight as still-RUNNING (the engine's normal update path) does not change its state.
		registry.register(newPlan(fresh, null, PlanState.DONE, 6));

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
		registry.register(newPlan(a, null, PlanState.DONE, 4));
		registry.register(newPlan(b, null, PlanState.DONE, 4));
		registry.register(newPlan(c, null, PlanState.DONE, 1));
		// d pushes total to 12 (>10). Eviction must drop `a` (4 nodes) → total 8 (<=10). It must NOT continue evicting
		// `b` since we are already under budget.
		registry.register(newPlan(d, null, PlanState.DONE, 3));

		Assertions.assertThat(registry.get(a)).isEmpty();
		Assertions.assertThat(registry.get(b)).isPresent();
		Assertions.assertThat(registry.get(c)).isPresent();
		Assertions.assertThat(registry.get(d)).isPresent();
		Assertions.assertThat(registry.totalNodeCount()).isEqualTo(8);
	}

	@Test
	public void testNegativeOrZeroBudgetIsRejected() {
		Assertions.assertThatThrownBy(() -> new BoundedQueryPlanRegistry(0))
				.isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThatThrownBy(() -> new BoundedQueryPlanRegistry(-1))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
