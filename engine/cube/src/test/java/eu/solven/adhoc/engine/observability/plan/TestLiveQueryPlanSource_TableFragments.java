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
	protected eu.solven.adhoc.engine.context.IQueryPreparator queryPreparator() {
		return eu.solven.adhoc.engine.context.StandardQueryPreparator.builder().queryPlanRegistry(registry).build();
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

		// Flat scan over the graph's nodes — at least one TABLE_QUERY node must be present (the fragment the
		// engine published for the executed merged query).
		long tableQueryNodes =
				plan.getNodes().stream().filter(n -> n.getOperator() == NodeOperator.TABLE_QUERY).count();
		Assertions.assertThat(tableQueryNodes)
				.as("Engine should have published at least one TABLE_QUERY fragment per table query executed")
				.isGreaterThanOrEqualTo(1);

		// And the table-query fragment carries the merged-query stats (rowsOut > 0 after completion).
		QueryPlanNode tableQueryNode = plan.getNodes()
				.stream()
				.filter(n -> n.getOperator() == NodeOperator.TABLE_QUERY)
				.findFirst()
				.orElseThrow();
		Assertions.assertThat(tableQueryNode.getState()).isEqualTo(NodeState.DONE);
		Assertions.assertThat(tableQueryNode.getStats().getRowsOut()).isPositive();
	}
}
