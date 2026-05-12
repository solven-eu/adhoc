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
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.options.StandardQueryOptions;
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
public class TestDagCubeQuery_DuckDb_Drillthrough extends ATestDagDuckDb implements IAdhocTestConstants {
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

		// DRILLTHROUGH on JOOQ-backed tables now goes through `streamRawRows`: the SQL has neither GROUP BY
		// nor aggregate function, so each source row produces one TabularEntry. The two `a=a1` rows
		// (k1=123 and k1=345) surface as TWO distinct entries — matching the InMemoryTable DT contract.
		Assertions.assertThat(view.getEntries()).hasSize(3).anySatisfy(entry -> {
			Assertions.assertThat((Map) entry.getCoordinates()).containsEntry("a", "a1");
			Assertions.assertThat((Map) entry.getValues()).containsEntry(k1Sum.getName(), 123D);
		}).anySatisfy(entry -> {
			Assertions.assertThat((Map) entry.getCoordinates()).containsEntry("a", "a1");
			Assertions.assertThat((Map) entry.getValues()).containsEntry(k1Sum.getName(), 345D);
		}).anySatisfy(entry -> {
			Assertions.assertThat((Map) entry.getCoordinates()).containsEntry("a", "a2");
			Assertions.assertThat((Map) entry.getValues()).containsEntry(k1Sum.getName(), 234D);
		});
	}

	// Reproducer for the bug where `COUNT(*)` (an aggregator whose columnName is the literal `*`) reaches the DT
	// ROWS path. `selectedRowsFields` builds `DSL.field(name(a.getColumnName()))` for every aggregator — for `*`
	// this becomes a reference to a wildcard, so the emitted SQL contains a literal `*` in the SELECT clause and
	// JOOQ expands it to every column of the underlying table. Consequences:
	// 1. With just COUNT(*): the row stream still returns 3 entries (since the wildcard expansion happens to
	// yield the original rows), but the alias is not attached to a meaningful per-row value.
	// 2. With COUNT(*) alongside another measure: the wildcard expansion pollutes the SELECT, causing values
	// to bind to the wrong aliases — this is the "values not attached to the correct column" symptom.
	// The SLICES path special-cases this via `buildCountAggregate` (DSL.count handles `*` correctly); the ROWS
	// path does not.
	@Test
	public void testDrillthrough_countAsterisk_alongsideSum_aliasesMustNotCrossWire() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		Aggregator countAll = Aggregator.countAsterisk();
		forest.addMeasure(countAll);
		forest.addMeasure(k1Sum);

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(countAll.getName(), k1Sum.getName())
				.groupByAlso("a")
				.option(StandardQueryOptions.DRILLTHROUGH)
				.build());

		ListMapEntryBasedTabularViewDrillThrough view = ListMapEntryBasedTabularViewDrillThrough.load(output);

		// 3 source rows expected. Each entry should carry the `k1` value under its own alias — and crucially
		// NOT under `count(*)`'s alias (the wildcard expansion bug would mis-bind values across aliases).
		Assertions.assertThat(view.getEntries()).hasSize(3);

		// The k1Sum alias must hold the matching k1 source-row value.
		Assertions.assertThat(view.getEntries())
				.anySatisfy(
						entry -> Assertions.assertThat((Map) entry.getValues()).containsEntry(k1Sum.getName(), 123D))
				.anySatisfy(
						entry -> Assertions.assertThat((Map) entry.getValues()).containsEntry(k1Sum.getName(), 234D))
				.anySatisfy(
						entry -> Assertions.assertThat((Map) entry.getValues()).containsEntry(k1Sum.getName(), 345D));

		// COUNT(*)'s per-row contribution is 1 (each row counts as 1). Every entry must have the count(*) alias
		// resolve to 1, NOT to a column value from the underlying table (which would indicate the wildcard
		// expansion mis-bound the alias).
		Assertions.assertThat(view.getEntries()).allSatisfy(entry -> {
			Object countValue = entry.getValues().get(countAll.getName());
			Assertions.assertThat(countValue)
					.as("COUNT(*) alias must hold the per-row counter (1), not a column value from `*` expansion")
					.isIn(1, 1L, 1D);
		});
	}
}
