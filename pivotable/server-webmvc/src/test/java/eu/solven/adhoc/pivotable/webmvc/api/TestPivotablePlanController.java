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
package eu.solven.adhoc.pivotable.webmvc.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import eu.solven.adhoc.engine.observability.plan.BoundedQueryPlanRegistry;
import eu.solven.adhoc.engine.observability.plan.IPlanSource;
import eu.solven.adhoc.engine.observability.plan.NodeOperator;
import eu.solven.adhoc.engine.observability.plan.NodeState;
import eu.solven.adhoc.engine.observability.plan.PlanState;
import eu.solven.adhoc.engine.observability.plan.QueryPlan;
import eu.solven.adhoc.engine.observability.plan.QueryPlanNode;
import eu.solven.adhoc.engine.observability.plan.QueryPlanSummary;
import eu.solven.adhoc.pivotable.query.AsynchronousStatus;
import eu.solven.adhoc.pivotable.query.PivotableAsynchronousQueriesManager;
import eu.solven.adhoc.query.AdhocQueryId;

/**
 * Unit tests for {@link PivotablePlanController}. The controller's four-state response contract (404 / 204+Retry-After
 * / 200 / 204) is exercised against a real {@link BoundedQueryPlanRegistry} and a stub async manager. Each state
 * asserts the HTTP code and the {@code Retry-After} header where it matters.
 */
public class TestPivotablePlanController {

	/** Minimal test fixture — wraps a pre-built {@link QueryPlan} as an {@link IPlanSource}. */
	private static IPlanSource sourceOf(QueryPlan plan) {
		return new IPlanSource() {
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
		};
	}

	/**
	 * Stub async manager: every call to {@link #getState(UUID)} returns whatever the test wrote into the
	 * {@code AtomicReference}. Sidesteps the cache-backed timing semantics of the real manager.
	 */
	private static PivotableAsynchronousQueriesManager stubManager(AtomicReference<AsynchronousStatus> stateRef) {
		return new PivotableAsynchronousQueriesManager() {
			@Override
			public AsynchronousStatus getState(UUID queryId) {
				return stateRef.get();
			}
		};
	}

	private static AdhocQueryId adhocId(UUID uuid) {
		return AdhocQueryId.builder().cube("test-cube").queryId(uuid).build();
	}

	private static QueryPlan plan(AdhocQueryId id) {
		QueryPlanNode root = QueryPlanNode.builder()
				.id("n0")
				.subject("root")
				.operator(NodeOperator.CUBE_STEP)
				.label("root")
				.state(NodeState.DONE)
				.build();
		return QueryPlan.builder()
				.queryId(id)
				.cubeName("test-cube")
				.state(PlanState.DONE)
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.completedAt(Instant.parse("2026-05-14T00:00:01Z"))
				.rootId("n0")
				.nodes(java.util.List.of(root))
				.nodeCount(1)
				.build();
	}

	@Test
	public void testSummary200WhenPlanRegistered() {
		// Engine has registered the plan → 200 with body.
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		UUID uuid = UUID.randomUUID();
		registry.registerSource(sourceOf(plan(adhocId(uuid))));

		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.SERVED)), registry);
		ResponseEntity<QueryPlanSummary> response = controller.getPlanSummary(uuid);

		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		Assertions.assertThat(response.getBody()).isNotNull();
		Assertions.assertThat(response.getBody().getState()).isEqualTo(PlanState.DONE);
	}

	@Test
	public void testSnapshot200WhenPlanRegistered() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		UUID uuid = UUID.randomUUID();
		registry.registerSource(sourceOf(plan(adhocId(uuid))));

		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.SERVED)), registry);
		ResponseEntity<QueryPlan> response = controller.getPlanSnapshot(uuid);

		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		Assertions.assertThat(response.getBody()).isNotNull();
	}

	@Test
	public void testSummary204WhenUuidUnknownToManager() {
		// Manager has never seen the UUID → 204 (no Retry-After). 404 is reserved for "endpoint doesn't exist";
		// an unknown UUID on a valid endpoint is still a valid no-content response.
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.UNKNOWN)), registry);

		ResponseEntity<QueryPlanSummary> response = controller.getPlanSummary(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
		Assertions.assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
	}

	@Test
	public void testSummary204WithRetryAfterWhenManagerKnowsButEngineNotReady() {
		// Manager accepted the submission, engine hasn't yet registered the plan → 204 + Retry-After.
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.RUNNING)), registry);

		ResponseEntity<QueryPlanSummary> response = controller.getPlanSummary(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
		Assertions.assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");
	}

	@Test
	public void testSummary204WithoutRetryAfterWhenEvictedAfterTermination() {
		// Manager marked it SERVED but the registry has nothing → 204 without Retry-After (terminal/gone).
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.SERVED)), registry);

		ResponseEntity<QueryPlanSummary> response = controller.getPlanSummary(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
		Assertions.assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
	}

	@Test
	public void testSnapshot204WhenUuidUnknownToManager() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.UNKNOWN)), registry);

		ResponseEntity<QueryPlan> response = controller.getPlanSnapshot(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
		Assertions.assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
	}

	@Test
	public void testChildren200ReturnsEmptyListForLeafQuery() {
		// Parent registered, no children → 200 with []. This is the normal case for non-composite queries.
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		UUID parentUuid = UUID.randomUUID();
		registry.registerSource(sourceOf(plan(adhocId(parentUuid))));

		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.SERVED)), registry);
		ResponseEntity<List<QueryPlanSummary>> response = controller.getPlanChildren(parentUuid);

		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		Assertions.assertThat(response.getBody()).isNotNull().isEmpty();
	}

	@Test
	public void testChildren200ReturnsOnePerChildForCompositeParent() {
		// Composite parent + 2 sub-cube children. The registry's `getChildrenOf` matches by parent UUID — set each
		// child's `parentQueryId` to the parent's UUID and assert both surface.
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		UUID parentUuid = UUID.randomUUID();
		AdhocQueryId parentId = adhocId(parentUuid);
		registry.registerSource(sourceOf(plan(parentId)));

		AdhocQueryId childA = adhocId(UUID.randomUUID());
		AdhocQueryId childB = adhocId(UUID.randomUUID());
		registry.registerSource(sourceOf(childPlanOf(childA, parentUuid)));
		registry.registerSource(sourceOf(childPlanOf(childB, parentUuid)));

		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.SERVED)), registry);
		ResponseEntity<List<QueryPlanSummary>> response = controller.getPlanChildren(parentUuid);

		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		Assertions.assertThat(response.getBody()).hasSize(2);
	}

	@Test
	public void testChildren204WhenParentUuidUnknown() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.UNKNOWN)), registry);

		ResponseEntity<List<QueryPlanSummary>> response = controller.getPlanChildren(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
		Assertions.assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
	}

	/** Build a plan whose parent is the given UUID. Mirrors {@link #plan} but sets a non-null parentQueryId. */
	private static QueryPlan childPlanOf(AdhocQueryId id, UUID parentUuid) {
		QueryPlanNode root = QueryPlanNode.builder()
				.id("n0")
				.subject("root")
				.operator(NodeOperator.CUBE_STEP)
				.label("root")
				.state(NodeState.DONE)
				.build();
		return QueryPlan.builder()
				.queryId(id)
				.parentQueryId(parentUuid)
				.cubeName("test-cube")
				.state(PlanState.DONE)
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.completedAt(Instant.parse("2026-05-14T00:00:01Z"))
				.rootId("n0")
				.nodes(java.util.List.of(root))
				.nodeCount(1)
				.build();
	}

	@Test
	public void testSnapshot204WithRetryAfterWhenQueuing() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.RUNNING)), registry);

		ResponseEntity<QueryPlan> response = controller.getPlanSnapshot(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
		Assertions.assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");
	}
}
