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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import eu.solven.adhoc.engine.observability.plan.BoundedQueryPlanRegistry;
import eu.solven.adhoc.engine.observability.plan.NodeOperator;
import eu.solven.adhoc.engine.observability.plan.NodeState;
import eu.solven.adhoc.engine.observability.plan.PlanState;
import eu.solven.adhoc.engine.observability.plan.QueryPlan;
import eu.solven.adhoc.engine.observability.plan.QueryPlanNode;
import eu.solven.adhoc.engine.observability.plan.QueryPlanSummary;
import eu.solven.adhoc.query.AdhocQueryId;

/**
 * Unit tests for {@link PivotablePlanController}. Use a real {@link BoundedQueryPlanRegistry} (cheap, no Spring boot)
 * to exercise the UUID-based lookup + 404 path.
 */
public class TestPivotablePlanController {

	private static AdhocQueryId newId() {
		return AdhocQueryId.builder().cube("test-cube").build();
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
	public void testSummaryReturnsRegisteredPlan() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId id = newId();
		registry.register(plan(id));

		PivotablePlanController controller = new PivotablePlanController(registry);
		ResponseEntity<QueryPlanSummary> response = controller.getPlanSummary(id.getQueryId());

		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		Assertions.assertThat(response.getBody()).isNotNull();
		Assertions.assertThat(response.getBody().getState()).isEqualTo(PlanState.DONE);
		Assertions.assertThat(response.getBody().getTotalNodes()).isEqualTo(1);
	}

	@Test
	public void testSnapshotReturnsRegisteredPlan() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		AdhocQueryId id = newId();
		registry.register(plan(id));

		PivotablePlanController controller = new PivotablePlanController(registry);
		ResponseEntity<QueryPlan> response = controller.getPlanSnapshot(id.getQueryId());

		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		Assertions.assertThat(response.getBody()).isNotNull();
		Assertions.assertThat(response.getBody().getQueryId().getQueryId()).isEqualTo(id.getQueryId());
	}

	@Test
	public void testSummary204OnUnknownUuid() {
		// Reserve 404 for "endpoint doesn't exist"; an unknown UUID (possibly evicted) returns 204 No Content.
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller = new PivotablePlanController(registry);

		ResponseEntity<QueryPlanSummary> response = controller.getPlanSummary(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
	}

	@Test
	public void testSnapshot204OnUnknownUuid() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		PivotablePlanController controller = new PivotablePlanController(registry);

		ResponseEntity<QueryPlan> response = controller.getPlanSnapshot(UUID.randomUUID());
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
	}
}
