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

import eu.solven.adhoc.engine.observability.plan.events.QueryPlanCompleted;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanFailed;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanNodeCompleted;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanNodeFailed;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanNodeStarted;
import eu.solven.adhoc.engine.observability.plan.events.QueryPlanRegistered;
import eu.solven.adhoc.query.AdhocQueryId;

public class TestQueryPlanRegistryUpdater {

	private static AdhocQueryId newId() {
		return AdhocQueryId.builder().cube("test-cube").build();
	}

	/** Build a small two-node plan: root with one child leaf. Subjects are simple strings keyed by content. */
	private static QueryPlan twoNodePlan(AdhocQueryId id, String rootSubject, String leafSubject) {
		QueryPlanNode leaf = QueryPlanNode.builder()
				.subject(leafSubject)
				.operator(NodeOperator.TABLE_QUERY)
				.label(leafSubject)
				.build();
		QueryPlanNode root = QueryPlanNode.builder()
				.subject(rootSubject)
				.operator(NodeOperator.CUBE_STEP)
				.label(rootSubject)
				.children(List.of(leaf))
				.build();
		return QueryPlan.builder()
				.queryId(id)
				.cubeName("test-cube")
				.submittedAt(Instant.parse("2026-05-13T00:00:00Z"))
				.root(root)
				.nodeCount(2)
				.build();
	}

	@Test
	public void testRegisteredStoresPlanInRegistry() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		QueryPlanRegistryUpdater updater = new QueryPlanRegistryUpdater(registry);
		AdhocQueryId id = newId();
		QueryPlan plan = twoNodePlan(id, "root", "leaf");

		updater.on(QueryPlanRegistered.builder().plan(plan).at(Instant.now()).fqdn("test").build());

		Assertions.assertThat(registry.get(id)).contains(plan);
	}

	@Test
	public void testNodeStartedFlipsStateAndPlan() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		QueryPlanRegistryUpdater updater = new QueryPlanRegistryUpdater(registry);
		AdhocQueryId id = newId();
		QueryPlan plan = twoNodePlan(id, "root", "leaf");
		Instant t0 = Instant.parse("2026-05-13T00:00:10Z");

		updater.on(QueryPlanRegistered.builder().plan(plan).at(t0.minusMillis(1)).fqdn("test").build());
		updater.on(QueryPlanNodeStarted.builder().queryId(id).subject("leaf").at(t0).fqdn("test").build());

		QueryPlan got = registry.get(id).orElseThrow();
		Assertions.assertThat(got.getState()).isEqualTo(PlanState.RUNNING);
		QueryPlanNode leaf = got.getRoot().getChildren().get(0);
		Assertions.assertThat(leaf.getState()).isEqualTo(NodeState.RUNNING);
		Assertions.assertThat(leaf.getStats().getStartedAt()).isEqualTo(t0);
	}

	@Test
	public void testNodeCompletedRecordsDurationAndRowCounts() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		QueryPlanRegistryUpdater updater = new QueryPlanRegistryUpdater(registry);
		AdhocQueryId id = newId();
		QueryPlan plan = twoNodePlan(id, "root", "leaf");
		Instant t0 = Instant.parse("2026-05-13T00:00:10Z");
		Instant t1 = t0.plusMillis(250);

		updater.on(QueryPlanRegistered.builder().plan(plan).at(t0.minusMillis(1)).fqdn("test").build());
		updater.on(QueryPlanNodeStarted.builder().queryId(id).subject("leaf").at(t0).fqdn("test").build());
		updater.on(QueryPlanNodeCompleted.builder()
				.queryId(id)
				.subject("leaf")
				.at(t1)
				.rowsIn(100L)
				.rowsOut(80L)
				.fqdn("test")
				.build());

		QueryPlanNode leaf = registry.get(id).orElseThrow().getRoot().getChildren().get(0);
		Assertions.assertThat(leaf.getState()).isEqualTo(NodeState.DONE);
		Assertions.assertThat(leaf.getStats().getCompletedAt()).isEqualTo(t1);
		Assertions.assertThat(leaf.getStats().getElapsedMs()).isEqualTo(250L);
		Assertions.assertThat(leaf.getStats().getRowsIn()).isEqualTo(100L);
		Assertions.assertThat(leaf.getStats().getRowsOut()).isEqualTo(80L);
	}

	@Test
	public void testNodeFailedRecordsErrorAndDuration() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		QueryPlanRegistryUpdater updater = new QueryPlanRegistryUpdater(registry);
		AdhocQueryId id = newId();
		QueryPlan plan = twoNodePlan(id, "root", "leaf");
		Instant t0 = Instant.parse("2026-05-13T00:00:10Z");

		updater.on(QueryPlanRegistered.builder().plan(plan).at(t0.minusMillis(1)).fqdn("test").build());
		updater.on(QueryPlanNodeStarted.builder().queryId(id).subject("leaf").at(t0).fqdn("test").build());
		updater.on(QueryPlanNodeFailed.builder()
				.queryId(id)
				.subject("leaf")
				.at(t0.plusMillis(50))
				.errorMessage("boom")
				.fqdn("test")
				.build());

		QueryPlanNode leaf = registry.get(id).orElseThrow().getRoot().getChildren().get(0);
		Assertions.assertThat(leaf.getState()).isEqualTo(NodeState.FAILED);
		Assertions.assertThat(leaf.getStats().getErrorMessage()).isEqualTo("boom");
		Assertions.assertThat(leaf.getStats().getElapsedMs()).isEqualTo(50L);
	}

	@Test
	public void testPlanCompletedTransitionsTopLevelState() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		QueryPlanRegistryUpdater updater = new QueryPlanRegistryUpdater(registry);
		AdhocQueryId id = newId();
		QueryPlan plan = twoNodePlan(id, "root", "leaf");
		Instant t0 = Instant.parse("2026-05-13T00:00:10Z");

		updater.on(QueryPlanRegistered.builder().plan(plan).at(t0).fqdn("test").build());
		updater.on(QueryPlanCompleted.builder().queryId(id).at(t0.plusSeconds(1)).fqdn("test").build());

		QueryPlan got = registry.get(id).orElseThrow();
		Assertions.assertThat(got.getState()).isEqualTo(PlanState.DONE);
		Assertions.assertThat(got.getCompletedAt()).isEqualTo(t0.plusSeconds(1));
	}

	@Test
	public void testPlanFailedTransitionsTopLevelState() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		QueryPlanRegistryUpdater updater = new QueryPlanRegistryUpdater(registry);
		AdhocQueryId id = newId();
		QueryPlan plan = twoNodePlan(id, "root", "leaf");
		Instant t0 = Instant.parse("2026-05-13T00:00:10Z");

		updater.on(QueryPlanRegistered.builder().plan(plan).at(t0).fqdn("test").build());
		updater.on(
				QueryPlanFailed.builder().queryId(id).at(t0.plusSeconds(1)).errorMessage("nope").fqdn("test").build());

		QueryPlan got = registry.get(id).orElseThrow();
		Assertions.assertThat(got.getState()).isEqualTo(PlanState.FAILED);
		Assertions.assertThat(got.getCompletedAt()).isEqualTo(t0.plusSeconds(1));
	}

	@Test
	public void testEventForUnknownQueryIsIgnoredGracefully() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		QueryPlanRegistryUpdater updater = new QueryPlanRegistryUpdater(registry);
		AdhocQueryId unknown = newId();

		// No prior register — the updater MUST NOT throw; it just logs and drops.
		updater.on(
				QueryPlanNodeStarted.builder().queryId(unknown).subject("leaf").at(Instant.now()).fqdn("test").build());
		updater.on(QueryPlanCompleted.builder().queryId(unknown).at(Instant.now()).fqdn("test").build());

		Assertions.assertThat(registry.get(unknown)).isEmpty();
	}

	@Test
	public void testEventForUnknownNodeInKnownPlanIsIgnoredGracefully() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		QueryPlanRegistryUpdater updater = new QueryPlanRegistryUpdater(registry);
		AdhocQueryId id = newId();
		QueryPlan plan = twoNodePlan(id, "root", "leaf");
		updater.on(QueryPlanRegistered.builder().plan(plan).at(Instant.now()).fqdn("test").build());

		// Unknown subject — no node matches. Updater must not throw; state stays unchanged.
		updater.on(QueryPlanNodeStarted.builder()
				.queryId(id)
				.subject("unknown-subject")
				.at(Instant.now())
				.fqdn("test")
				.build());

		Assertions.assertThat(registry.get(id).orElseThrow().getState()).isEqualTo(PlanState.PENDING);
		Assertions.assertThat(registry.get(id).orElseThrow().getRoot().getState()).isEqualTo(NodeState.PENDING);
	}

	@Test
	public void testFullLifecycleEndToEnd() {
		BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(100);
		QueryPlanRegistryUpdater updater = new QueryPlanRegistryUpdater(registry);
		AdhocQueryId id = newId();
		QueryPlan plan = twoNodePlan(id, "root", "leaf");
		Instant t = Instant.parse("2026-05-13T00:00:00Z");

		updater.on(QueryPlanRegistered.builder().plan(plan).at(t).fqdn("test").build());
		updater.on(QueryPlanNodeStarted.builder().queryId(id).subject("root").at(t.plusMillis(1)).fqdn("test").build());
		updater.on(QueryPlanNodeStarted.builder().queryId(id).subject("leaf").at(t.plusMillis(2)).fqdn("test").build());
		updater.on(QueryPlanNodeCompleted.builder()
				.queryId(id)
				.subject("leaf")
				.at(t.plusMillis(102))
				.rowsOut(50L)
				.fqdn("test")
				.build());
		updater.on(QueryPlanNodeCompleted.builder()
				.queryId(id)
				.subject("root")
				.at(t.plusMillis(110))
				.rowsOut(20L)
				.fqdn("test")
				.build());
		updater.on(QueryPlanCompleted.builder().queryId(id).at(t.plusMillis(120)).fqdn("test").build());

		QueryPlan got = registry.get(id).orElseThrow();
		Assertions.assertThat(got.getState()).isEqualTo(PlanState.DONE);
		Assertions.assertThat(got.getRoot().getState()).isEqualTo(NodeState.DONE);
		Assertions.assertThat(got.getRoot().getStats().getRowsOut()).isEqualTo(20L);
		Assertions.assertThat(got.getRoot().getChildren().get(0).getState()).isEqualTo(NodeState.DONE);
		Assertions.assertThat(got.getRoot().getChildren().get(0).getStats().getRowsOut()).isEqualTo(50L);
		Assertions.assertThat(got.getRoot().getChildren().get(0).getStats().getElapsedMs()).isEqualTo(100L);
	}
}
