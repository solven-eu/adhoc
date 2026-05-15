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
package eu.solven.adhoc.table.duckdb;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.observability.plan.BoundedQueryPlanRegistry;
import eu.solven.adhoc.engine.observability.plan.NodeOperator;
import eu.solven.adhoc.engine.observability.plan.QueryPlan;
import eu.solven.adhoc.engine.observability.plan.QueryPlanNode;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.SubmittedQueryIdScope;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

/**
 * End-to-end test that the SQL fragment posted by {@link JooqTableWrapper} reaches the {@link BoundedQueryPlanRegistry}
 * via the cube engine's wiring, and that the resulting plan tree carries an inlined SQL leaf under each
 * {@code TABLE_QUERY} node.
 *
 * <p>
 * Uses DuckDB through {@link JooqTableWrapper} (the wrapper that owns the SQL-leaf publication path). The SQL text is
 * asserted on substring rather than exact match so jOOQ's dialect-specific rendering can evolve without breaking the
 * test.
 *
 * @author Benoit Lacelle
 */
public class TestDagCubeQuery_DuckDB_SqlFragment extends ATestDagDuckDb implements IAdhocTestConstants {

	String tableName = "sqlFragmentTable";

	BoundedQueryPlanRegistry registry = new BoundedQueryPlanRegistry(10_000);

	@Override
	public CubeQueryEngine engine() {
		return CubeQueryEngine.builder()
				.eventBus(eventBus())
				.factories(makeFactories())
				.queryPlanRegistry(registry)
				.build();
	}

	@Override
	public ITableWrapper makeTable() {
		// Production wiring uses the plain 2-arg constructor (no registry plumbed through the wrapper). The cube
		// engine populates `QueryPod.queryPlanRegistry` just-in-time so the wrapper pulls the right registry at
		// `streamSlices` time. Use the same here to match the realistic call shape.
		return new JooqTableWrapper(tableName,
				DuckDBHelper.parametersBuilder(dslSupplier).tableName(tableName).build());
	}

	@BeforeEach
	public void initDataAndMeasures() {
		dsl.createTableIfNotExists(tableName)
				.column("I", SQLDataType.BIGINT)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("I"), DSL.field("k1")).values(1, 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("I"), DSL.field("k1")).values(12, 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("I"), DSL.field("k1")).values(21, 345).execute();

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testSqlFragmentLandsAsLeafUnderTableQueryNode() {
		// Pre-generate a UUID + bind via SubmittedQueryIdScope so the engine adopts it as the query's id. The test
		// then knows the registry key without poking package-private internals.
		java.util.UUID submittedUuid = java.util.UUID.randomUUID();
		SubmittedQueryIdScope.runWith(submittedUuid,
				() -> cube().execute(CubeQuery.builder().measure(k1Sum.getName()).build()));

		Assertions.assertThat(registry.planCount()).isEqualTo(1);
		AdhocQueryId queryId = registry.findIdByUuid(submittedUuid).orElseThrow();
		QueryPlan plan = registry.snapshot(queryId).orElseThrow();

		// Find a TABLE_QUERY node that itself has a TABLE_QUERY child carrying the SQL details — the table-engine
		// publishes the parent fragment (anchored on TableQueryStep), the wrapper publishes the leaf (anchored on
		// the TableQueryV4). The two-layer chain is exactly the design specified in CHANGES.MD.
		QueryPlanNode sqlLeaf = findSqlLeaf(plan.getRoot());
		Assertions.assertThat(sqlLeaf)
				.as("Expected a SQL leaf grafted under the TABLE_QUERY node — JooqTableWrapper.publishSqlFragment")
				.isNotNull();
		Assertions.assertThat(sqlLeaf.getDetails()).containsEntry("language", "sql");

		String sql = sqlLeaf.getDetails().get("sql");
		Assertions.assertThat(sql).isNotBlank();
		// jOOQ's INLINED-mode output for a simple SUM(k1) on DuckDB must contain at least the aggregate and the
		// table reference. Exact rendering varies with dialect / version, hence substring matching.
		Assertions.assertThat(sql.toLowerCase()).contains("sum(").contains(tableName.toLowerCase());

		// The label is a one-line SQL PREVIEW — renderers that show only `label` (e.g. Pivotable's Mermaid
		// graph) get a tangible hint at the query shape instead of a generic "sql" string. Capped at
		// {@link JooqTableWrapper#SQL_LABEL_MAX_CHARS} so long jOOQ output doesn't blow up the graph node;
		// full SQL stays in `details.sql` for the modal's copy-to-clipboard button.
		Assertions.assertThat(sqlLeaf.getLabel().toLowerCase()).contains("sum(");
		// Whitespace is collapsed in the label so pretty-printed SQL fits on one line.
		Assertions.assertThat(sqlLeaf.getLabel()).doesNotContain("\n").doesNotContain("\t");
		// Length cap is enforced — short SQL passes through unchanged, long SQL ends with an ellipsis.
		Assertions.assertThat(sqlLeaf.getLabel().length()).isLessThanOrEqualTo(81);
		if (sqlLeaf.getLabel().length() == 81) {
			Assertions.assertThat(sqlLeaf.getLabel()).endsWith("…");
		}
	}

	/**
	 * Depth-first search for the SQL-leaf node. The leaf is identified by its `details.sql` entry — we don't try to
	 * match on operator / subject because the wrapper uses an internal sentinel subject we don't want to leak into the
	 * test.
	 */
	private static QueryPlanNode findSqlLeaf(QueryPlanNode node) {
		if (node.getOperator() == NodeOperator.TABLE_QUERY && node.getDetails().containsKey("sql")) {
			return node;
		}
		for (QueryPlanNode child : node.getChildren()) {
			QueryPlanNode hit = findSqlLeaf(child);
			if (hit != null) {
				return hit;
			}
		}
		return null;
	}
}
