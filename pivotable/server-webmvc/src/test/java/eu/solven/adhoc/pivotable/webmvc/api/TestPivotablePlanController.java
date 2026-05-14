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
				.root(root)
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
	public void testSummary404WhenUuidUnknownToManager() {
		// Manager has never seen the UUID → 404 (typo / stale link).
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.UNKNOWN)), registry);

		ResponseEntity<QueryPlanSummary> response = controller.getPlanSummary(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
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
	public void testSnapshot404WhenUuidUnknownToManager() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller =
				new PivotablePlanController(stubManager(new AtomicReference<>(AsynchronousStatus.UNKNOWN)), registry);

		ResponseEntity<QueryPlan> response = controller.getPlanSnapshot(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
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
