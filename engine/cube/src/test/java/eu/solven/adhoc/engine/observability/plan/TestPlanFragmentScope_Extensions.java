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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.SubmittedQueryIdScope;

/**
 * Demonstrates the boiler-plate-free extension scenarios enabled by {@link PlanFragmentScope}: a third-party
 * {@link ICombination} (or, by the same token, a calculated column / routing measure) can publish a plan-fragment leaf
 * describing its own behaviour without having to learn the {@link IQueryPlanRegistry} / {@link AdhocQueryId} pair.
 *
 * <p>
 * Two scenarios are pinned here:
 * <ol>
 * <li><strong>"Combinator publishes a sub-node"</strong> — the combinator emits a single leaf carrying its name + the
 * size of the underlying input list as a free-form detail. The plan-tree afterward shows the leaf grafted under the
 * combinator's cube step.</li>
 * <li><strong>"Calculated-column-style SQL fragment"</strong> — the combinator (standing in for a calculated column)
 * publishes a leaf with {@code details.language = "sql"} and a {@code details.sql} string. Same shape as
 * {@code JooqTableWrapper.publishSqlFragment} but driven by the extension code, anchored on the cube step the extension
 * serves.</li>
 * </ol>
 *
 * @author Benoit Lacelle
 */
public class TestPlanFragmentScope_Extensions extends ATestDagInMemory {

	BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10_000);

	@Override
	protected eu.solven.adhoc.engine.context.IQueryPreparator queryPreparator() {
		return eu.solven.adhoc.engine.context.StandardQueryPreparator.builder().queryPlanRegistry(registry).build();
	}

	@BeforeEach
	@Override
	public void feedTable() {
		table().add(Map.of("k1", 1));
		table().add(Map.of("k1", 2));
		table().add(Map.of("k1", 3));

		forest.addMeasure(Aggregator.sum("k1"));
		forest.addMeasure(Combinator.builder()
				.name("decorated")
				.underlying("k1")
				.combinationKey(PlanPublishingCombination.class.getName())
				.build());
	}

	/**
	 * The combinator publishes a free-form leaf on every {@code combine()} call. The plan snapshot afterward must show
	 * the leaf grafted under the combinator's own step.
	 */
	@Test
	public void testCombinatorPublishesSubNode() {
		PlanPublishingCombination.lastSqlPublished = null;

		UUID submittedUuid = UUID.randomUUID();
		SubmittedQueryIdScope.runWith(submittedUuid,
				() -> cube().execute(CubeQuery.builder().measure("decorated").build()));

		AdhocQueryId queryId = registry.findIdByUuid(submittedUuid).orElseThrow();
		QueryPlan plan = registry.snapshot(queryId).orElseThrow();

		// Scan the flat nodes list for our combinator's marker. Operator is OTHER (extension-defined).
		QueryPlanNode leaf = findLeaf(plan, node -> node.getDetails().containsKey("combinator-name"));
		Assertions.assertThat(leaf)
				.as("Combinator must have published its plan-fragment leaf via PlanFragmentScope.current()")
				.isNotNull();
		Assertions.assertThat(leaf.getOperator()).isEqualTo(NodeOperator.OTHER);
		Assertions.assertThat(leaf.getDetails())
				.containsEntry("combinator-name", "decorated")
				.containsKey("underlying-count");
	}

	/**
	 * Same combinator also publishes a SQL-leaf-style fragment — same shape as what a calculated column relying on a
	 * sub-query would publish.
	 */
	@Test
	public void testCalculatedColumnStyleSqlLeafIsAccepted() {
		PlanPublishingCombination.lastSqlPublished = "SELECT count(*) FROM helper_table WHERE k1 IS NOT NULL";

		UUID submittedUuid = UUID.randomUUID();
		SubmittedQueryIdScope.runWith(submittedUuid,
				() -> cube().execute(CubeQuery.builder().measure("decorated").build()));

		AdhocQueryId queryId = registry.findIdByUuid(submittedUuid).orElseThrow();
		QueryPlan plan = registry.snapshot(queryId).orElseThrow();

		QueryPlanNode sqlLeaf = findLeaf(plan, node -> "sql".equals(node.getDetails().get("language")));
		Assertions.assertThat(sqlLeaf)
				.as("Combinator's SQL-style leaf should appear when the publish call runs with a non-null sql")
				.isNotNull();
		Assertions.assertThat(sqlLeaf.getDetails())
				.containsEntry("language", "sql")
				.containsEntry("sql", "SELECT count(*) FROM helper_table WHERE k1 IS NOT NULL");
	}

	/**
	 * Re-publishing the same {@code leafKey} replaces rather than appends — the contract that lets extensions publish
	 * on every {@code combine()} call without growing the fragment list unboundedly.
	 */
	@Test
	public void testRePublishWithSameLeafKeyDoesNotAccumulate() {
		PlanPublishingCombination.lastSqlPublished = null;

		UUID submittedUuid = UUID.randomUUID();
		SubmittedQueryIdScope.runWith(submittedUuid,
				() -> cube().execute(CubeQuery.builder()
						.measure("decorated")
						// Group by a column with several distinct values → combinator fires multiple times.
						.groupByAlso("k1")
						.build()));

		AdhocQueryId queryId = registry.findIdByUuid(submittedUuid).orElseThrow();
		QueryPlan plan = registry.snapshot(queryId).orElseThrow();

		// The dedup contract: even if combine() ran N times, only ONE leaf with this combinator-name is grafted.
		long matchingLeaves = countLeaves(plan, node -> node.getDetails().containsKey("combinator-name"));
		Assertions.assertThat(matchingLeaves)
				.as("PlanFragmentScope leaf with stable subject must dedup across multiple combine() invocations")
				.isEqualTo(1L);
	}

	private static QueryPlanNode findLeaf(QueryPlan plan, java.util.function.Predicate<QueryPlanNode> matcher) {
		return plan.getNodes().stream().filter(matcher).findFirst().orElse(null);
	}

	private static long countLeaves(QueryPlan plan, java.util.function.Predicate<QueryPlanNode> matcher) {
		return plan.getNodes().stream().filter(matcher).count();
	}

	/**
	 * Combinator that, on every {@code combine()} call, publishes a plan-fragment leaf via the ambient sink. Used here
	 * as a stand-in for a calculated column, routing measure, or any extension that wants its work surfaced in the plan
	 * tree.
	 */
	public static class PlanPublishingCombination implements ICombination {

		/**
		 * Static toggle for the optional SQL leaf — set by the test before invoking the cube. Could just as well be a
		 * constructor arg if the combinator weren't instantiated reflectively by the engine.
		 */
		static volatile String lastSqlPublished;

		@Override
		public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
			IPlanFragmentSink sink = PlanFragmentScope.current();

			Object cubeStep = slice.getQueryStep();

			// Free-form "I ran on this step" leaf. The leafKey is the (cubeStep, "combinator-name") pair so the
			// fragment dedups across multiple combine() invocations on the same step.
			sink.publishDoneLeaf(cubeStep,
					new LeafKey(cubeStep, "combinator-name"),
					NodeOperator.OTHER,
					"decorated combinator",
					Map.of("combinator-name",
							"decorated",
							"underlying-count",
							Integer.toString(underlyingValues.size())));

			// Optional SQL-style leaf — exercises the same path a calculated column with a sub-query would.
			String sql = lastSqlPublished;
			if (sql != null) {
				sink.publishDoneLeaf(cubeStep,
						new LeafKey(cubeStep, "calculated-column-sql"),
						NodeOperator.OTHER,
						"calculated-column",
						Map.of("language", "sql", "sql", sql));
			}

			if (underlyingValues.isEmpty()) {
				return null;
			} else {
				return underlyingValues.get(0);
			}
		}

		// Stable value-equals subject so re-publishing dedups inside the LiveQueryPlanSource's fragments-by-anchor.
		private record LeafKey(Object anchor, String tag) {
		}
	}
}
