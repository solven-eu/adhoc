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

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.ListMapEntryBasedTabularViewDrillThrough;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

/**
 * DRILLTHROUGH against a DuckDB-backed JOOQ table. Reproducer for the bug where {@code mergeForDrillthrough} rewrites
 * every aggregator to {@link eu.solven.adhoc.measure.sum.CoalesceAggregation} and
 * {@link eu.solven.adhoc.table.sql.JooqTableQueryFactory#buildAggregateFunction} has no SQL mapping for the
 * {@code COALESCE} aggregation key — it falls through to {@code onCustomAggregation} which throws
 * {@code UnsupportedOperationException("SQL does not support aggregationKey=COALESCE")}.
 */
public class TestCubeQuery_DuckDb_Drillthrough extends ADuckDbJooqTest implements IAdhocTestConstants {
	String tableName = "duckdb_drillthrough";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				DuckDBHelper.parametersBuilder(dslSupplier).tableName(tableName).build());
	}

	@Test
	public void testDrillthrough_groupByA_singleMeasure() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		forest.addMeasure(k1Sum);

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(k1Sum.getName())
				.groupByAlso("a")
				.option(StandardQueryOptions.DRILLTHROUGH)
				.build());

		ListMapEntryBasedTabularViewDrillThrough view = ListMapEntryBasedTabularViewDrillThrough.load(output);

		// DuckDB renders the merged aggregator as `any_value(k1)` and applies a `GROUP BY a` at the SQL
		// layer, so the table returns ONE row per slice (two slices: a=a1, a=a2) — not one row per source
		// row. The two a=a1 rows (k1=123, k1=345) collapse to a single slice; DuckDB picks any of the two
		// `k1` values (we only assert membership). This is a deliberate divergence from the
		// InMemoryTable DT contract (per-source-row records); SQL backends inherently group at the engine
		// level. Pinning the per-slice shape here, with a TODO to discuss whether DT-on-SQL should bypass
		// GROUP BY entirely (e.g. by stripping the groupBy in `mergeForDrillthrough` for SQL tables).
		Assertions.assertThat(view.getEntries()).hasSize(2).anySatisfy(entry -> {
			Assertions.assertThat((Map) entry.getCoordinates()).containsEntry("a", "a1");
			Assertions.assertThat((Map) entry.getValues())
					.hasEntrySatisfying(k1Sum.getName(), v -> Assertions.assertThat(v).isIn(123D, 345D));
		}).anySatisfy(entry -> {
			Assertions.assertThat((Map) entry.getCoordinates()).containsEntry("a", "a2");
			Assertions.assertThat((Map) entry.getValues()).containsEntry(k1Sum.getName(), 234D);
		});
	}
}
