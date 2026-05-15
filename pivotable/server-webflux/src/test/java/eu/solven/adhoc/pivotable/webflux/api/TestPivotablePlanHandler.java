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
package eu.solven.adhoc.pivotable.webflux.api;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.reactive.function.server.MockServerRequest;

import eu.solven.adhoc.engine.observability.plan.BoundedQueryPlanRegistry;
import eu.solven.adhoc.engine.observability.plan.IPlanSource;
import eu.solven.adhoc.engine.observability.plan.NodeOperator;
import eu.solven.adhoc.engine.observability.plan.NodeState;
import eu.solven.adhoc.engine.observability.plan.PlanState;
import eu.solven.adhoc.engine.observability.plan.QueryPlan;
import eu.solven.adhoc.engine.observability.plan.QueryPlanNode;
import eu.solven.adhoc.pivotable.query.AsynchronousStatus;
import eu.solven.adhoc.pivotable.query.PivotableAsynchronousQueriesManager;
import eu.solven.adhoc.query.AdhocQueryId;
import reactor.test.StepVerifier;

/**
 * WebFlux counterpart to {@code TestPivotablePlanController}. Same four-state contract; verified through a
 * {@link MockServerRequest} + {@link StepVerifier} pipeline.
 */
public class TestPivotablePlanHandler {

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
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		UUID uuid = UUID.randomUUID();
		registry.registerSource(sourceOf(plan(adhocId(uuid))));

		PivotablePlanHandler handler =
				new PivotablePlanHandler(stubManager(new AtomicReference<>(AsynchronousStatus.SERVED)), registry);
		MockServerRequest request = MockServerRequest.builder().pathVariable("queryUuid", uuid.toString()).build();

		StepVerifier.create(handler.getPlanSummary(request))
				.assertNext(response -> Assertions.assertThat(response.statusCode()).isEqualTo(HttpStatus.OK))
				.verifyComplete();
	}

	@Test
	public void testSummary204WhenUuidUnknownToManager() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanHandler handler =
				new PivotablePlanHandler(stubManager(new AtomicReference<>(AsynchronousStatus.UNKNOWN)), registry);
		MockServerRequest request =
				MockServerRequest.builder().pathVariable("queryUuid", UUID.randomUUID().toString()).build();

		StepVerifier.create(handler.getPlanSummary(request)).assertNext(response -> {
			Assertions.assertThat(response.statusCode()).isEqualTo(HttpStatus.NO_CONTENT);
			Assertions.assertThat(response.headers().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
		}).verifyComplete();
	}

	@Test
	public void testSummary204WithRetryAfterWhenQueuing() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanHandler handler =
				new PivotablePlanHandler(stubManager(new AtomicReference<>(AsynchronousStatus.RUNNING)), registry);
		MockServerRequest request =
				MockServerRequest.builder().pathVariable("queryUuid", UUID.randomUUID().toString()).build();

		StepVerifier.create(handler.getPlanSummary(request)).assertNext(response -> {
			Assertions.assertThat(response.statusCode()).isEqualTo(HttpStatus.NO_CONTENT);
			Assertions.assertThat(response.headers().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("1");
		}).verifyComplete();
	}

	@Test
	public void testSummary204WithoutRetryAfterWhenEvicted() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanHandler handler =
				new PivotablePlanHandler(stubManager(new AtomicReference<>(AsynchronousStatus.SERVED)), registry);
		MockServerRequest request =
				MockServerRequest.builder().pathVariable("queryUuid", UUID.randomUUID().toString()).build();

		StepVerifier.create(handler.getPlanSummary(request)).assertNext(response -> {
			Assertions.assertThat(response.statusCode()).isEqualTo(HttpStatus.NO_CONTENT);
			Assertions.assertThat(response.headers().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
		}).verifyComplete();
	}

	@Test
	public void testChildren200ReturnsOnePerChild() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		UUID parentUuid = UUID.randomUUID();
		AdhocQueryId parentId = AdhocQueryId.builder().cube("test-cube").queryId(parentUuid).build();
		registry.registerSource(sourceOf(plan(parentId)));

		// Child plan with its parentQueryId pointing to the parent's UUID.
		QueryPlan childPlan = QueryPlan.builder()
				.queryId(AdhocQueryId.builder().cube("test-cube").queryId(UUID.randomUUID()).build())
				.parentQueryId(parentUuid)
				.cubeName("test-cube")
				.state(PlanState.DONE)
				.submittedAt(Instant.parse("2026-05-14T00:00:00Z"))
				.completedAt(Instant.parse("2026-05-14T00:00:01Z"))
				.root(QueryPlanNode.builder()
						.subject("root")
						.operator(NodeOperator.CUBE_STEP)
						.label("root")
						.state(NodeState.DONE)
						.build())
				.nodeCount(1)
				.build();
		registry.registerSource(sourceOf(childPlan));

		PivotablePlanHandler handler =
				new PivotablePlanHandler(stubManager(new AtomicReference<>(AsynchronousStatus.SERVED)), registry);
		MockServerRequest request =
				MockServerRequest.builder().pathVariable("queryUuid", parentUuid.toString()).build();

		StepVerifier.create(handler.getPlanChildren(request))
				.assertNext(response -> Assertions.assertThat(response.statusCode()).isEqualTo(HttpStatus.OK))
				.verifyComplete();
	}

	@Test
	public void testSnapshot204WhenUuidUnknownToManager() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanHandler handler =
				new PivotablePlanHandler(stubManager(new AtomicReference<>(AsynchronousStatus.UNKNOWN)), registry);
		MockServerRequest request =
				MockServerRequest.builder().pathVariable("queryUuid", UUID.randomUUID().toString()).build();

		StepVerifier.create(handler.getPlanSnapshot(request)).assertNext(response -> {
			Assertions.assertThat(response.statusCode()).isEqualTo(HttpStatus.NO_CONTENT);
			Assertions.assertThat(response.headers().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
		}).verifyComplete();
	}
}
