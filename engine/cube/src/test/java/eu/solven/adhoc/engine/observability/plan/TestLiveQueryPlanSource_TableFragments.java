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

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.SubmittedQueryIdScope;

/**
 * End-to-end test that the cube engine → table engine fragment-publication chain reaches the
 * {@link BoundedQueryPlanRegistry} and the resulting snapshot exposes a {@code TABLE_QUERY} child grafted under every
 * leaf cube node that ran a {@code TableQueryV4}.
 *
 * <p>
 * Uses {@code InMemoryTable} (not jOOQ) — that exercises the table-engine publication path, not the wrapper-side SQL
 * leaf. SQL leaves are tested separately in {@code TestJooqTableWrapper_PlanFragments}.
 *
 * @author Benoit Lacelle
 */
public class TestLiveQueryPlanSource_TableFragments extends ATestDagInMemory {

	BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10_000);

	@Override
	public CubeQueryEngine engine() {
		return CubeQueryEngine.builder()
				.eventBus(eventBus())
				.factories(makeFactories())
				.queryPlanRegistry(registry)
				.build();
	}

	@BeforeEach
	@Override
	public void feedTable() {
		table().add(Map.of("k1", 100));
		table().add(Map.of("k1", 200));
		forest.addMeasure(Aggregator.sum("k1"));
	}

	@Test
	public void testTableQueryV4FragmentIsGraftedUnderLeafAggregatorNode() {
		// Pre-generate the queryId so we can look it up by uuid below — avoids cross-package access to the
		// registry's package-private maps.
		java.util.UUID submittedUuid = java.util.UUID.randomUUID();
		SubmittedQueryIdScope.runWith(submittedUuid, () -> cube().execute(CubeQuery.builder().measure("k1").build()));

		Assertions.assertThat(registry.planCount()).isEqualTo(1);
		AdhocQueryId queryId = registry.findIdByUuid(submittedUuid).orElseThrow();
		QueryPlan plan = registry.snapshot(queryId).orElseThrow();
		Assertions.assertThat(plan.getState()).isEqualTo(PlanState.DONE);

		// Walk the tree, looking for at least one TABLE_QUERY node. The exact tree shape depends on the cube DAG
		// (single measure → single root → single leaf), but the fragment must appear under SOME node.
		long tableQueryNodes = countByOperator(plan.getRoot(), NodeOperator.TABLE_QUERY);
		Assertions.assertThat(tableQueryNodes)
				.as("Engine should have published at least one TABLE_QUERY fragment per table query executed")
				.isGreaterThanOrEqualTo(1);

		// And the table-query fragment carries the merged-query stats (rowsOut > 0 after completion).
		QueryPlanNode tableQueryNode = findFirstByOperator(plan.getRoot(), NodeOperator.TABLE_QUERY);
		Assertions.assertThat(tableQueryNode).isNotNull();
		Assertions.assertThat(tableQueryNode.getState()).isEqualTo(NodeState.DONE);
		Assertions.assertThat(tableQueryNode.getStats().getRowsOut()).isPositive();
	}

	private long countByOperator(QueryPlanNode node, NodeOperator op) {
		long c;
		if (node.getOperator() == op) {
			c = 1L;
		} else {
			c = 0L;
		}
		for (QueryPlanNode child : node.getChildren()) {
			c += countByOperator(child, op);
		}
		return c;
	}

	private QueryPlanNode findFirstByOperator(QueryPlanNode node, NodeOperator op) {
		if (node.getOperator() == op) {
			return node;
		}
		for (QueryPlanNode child : node.getChildren()) {
			QueryPlanNode hit = findFirstByOperator(child, op);
			if (hit != null) {
				return hit;
			}
		}
		return null;
	}
}
