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
import org.jooq.TableLike;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSetMultimap;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.IJooqColumnsResolver;
import eu.solven.adhoc.table.sql.IJooqTableSupplier;
import eu.solven.adhoc.table.sql.JooqColumnsHelpers;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqTableSupplier;
import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqTableSupplierBuilder;
import eu.solven.pepper.collection.MapWithNulls;

/**
 * End-to-end test of {@link PrunedJoinsJooqTableSupplierBuilder} against an in-memory DuckDB backend.
 * <p>
 * The schema is a 3-level snowflake:
 *
 * <pre>
 * fact  →  product  →  country
 *    (productId)    (countryId)
 * </pre>
 *
 * Each query is routed through {@code JooqTableWrapper} with the builder's {@link PrunedJoinsJooqTableSupplier} wired
 * into {@code JooqTableWrapperParameters.tableSupplier}. Behaviour mirrors {@code TestTableQuery_DuckDb_withJoin}, but
 * the {@code FROM} clause is pruned per query — a claim we verify by rendering the per-query table directly.
 *
 * @author Benoit Lacelle
 */
public class TestTableQuery_DuckDb_withPrunedJoins extends ADuckDbJooqTest implements IAdhocTestConstants {

	String factTable = "pruned_fact";
	String productTable = "pruned_product";
	String countryTable = "pruned_country";

	/**
	 * Single columns-resolver instance shared between {@link JooqTableWrapper} (via parameters) and
	 * {@link PrunedJoinsJooqTableSupplier}. Any customisation propagates to both paths.
	 */
	IJooqColumnsResolver columnsResolver = JooqColumnsHelpers.dbProbe();

	/** Schema side: records the join tree. */
	PrunedJoinsJooqTableSupplierBuilder snowflakeBuilder = PrunedJoinsJooqTableSupplierBuilder.prunedBuilder()
			.baseTable(DSL.table(DSL.name(factTable)))
			.baseTableAlias("f")
			.build()
			// fact → product: non-key columns `productName` + `countryId` (countryId is used for the next snowflake
			// leg). Star: defaults to base `f`.
			.leftJoin(j -> j.table(DSL.table(DSL.name(productTable))).alias("p").onSame("productId"))
			// product → country (snowflake chain): non-key column `countryName`. `.from("p")` switches the parent.
			.leftJoin(j -> j.table(DSL.table(DSL.name(countryTable))).alias("c").from("p").onSame("countryId"));

	/**
	 * Pruning side: per-query {@link IJooqTableSupplier}. {@code DSL.table(Name)} carries no declared fields, so we
	 * wire a DB-backed {@code SELECT * LIMIT 0} probe. Probing is lazy — it happens on the first query, after
	 * {@code initTables()} has created the DuckDB schema.
	 */
	PrunedJoinsJooqTableSupplier tableSupplier = PrunedJoinsJooqTableSupplier.builder()
			.dslSupplier(dslSupplier)
			.schema(snowflakeBuilder)
			.columnsResolver(columnsResolver)
			.build();

	@Override
	public ITableWrapper makeTable() {
		JooqTableWrapperParameters params = DuckDBHelper.parametersBuilder(dslSupplier)
				// `tableSupplier(...)` wires both the per-query pruning supplier and (via `getSchemaTable()`) the
				// all-joins table used by `getColumns` / `getResultForFields`.
				.tableSupplier(tableSupplier)
				// Shared probe — `getColumns()` uses it; the supplier already uses the same instance.
				.columnsResolver(columnsResolver)
				.build();
		return new JooqTableWrapper(factTable, params);
	}

	private void initTables() {
		dsl.createTableIfNotExists(factTable)
				.column("k1", SQLDataType.INTEGER)
				.column("productId", SQLDataType.VARCHAR)
				.execute();
		dsl.createTableIfNotExists(productTable)
				.column("productId", SQLDataType.VARCHAR)
				.column("productName", SQLDataType.VARCHAR)
				.column("countryId", SQLDataType.VARCHAR)
				.execute();
		dsl.createTableIfNotExists(countryTable)
				.column("countryId", SQLDataType.VARCHAR)
				.column("countryName", SQLDataType.VARCHAR)
				.execute();
	}

	private void insertData() {
		dsl.insertInto(DSL.table(factTable), DSL.field("k1"), DSL.field("productId")).values(123, "carot").execute();
		dsl.insertInto(DSL.table(factTable), DSL.field("k1"), DSL.field("productId")).values(234, "banana").execute();
		dsl.insertInto(DSL.table(productTable),
				DSL.field("productId"),
				DSL.field("productName"),
				DSL.field("countryId")).values("carot", "Carotte", "FRA").execute();
		dsl.insertInto(DSL.table(countryTable), DSL.field("countryId"), DSL.field("countryName"))
				.values("FRA", "France")
				.execute();
	}

	/** Builds a minimal V4 query used only to inspect the pruning decision (not executed). */
	private static TableQueryV4 v4GroupBy(String column) {
		IGroupBy gb = GroupByColumns.named(column);
		FilteredAggregator agg = FilteredAggregator.builder().aggregator(Aggregator.sum("k1")).build();
		return TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gb, agg)).build();
	}

	// ── 1. Grand-total: no join needed ─────────────────────────────────────

	@Test
	public void testGrandTotal_prunesAllJoins() {
		initTables();
		insertData();
		forest.addMeasure(k1Sum);

		// The pruned table for this query should be just the base — zero joins.
		TableLike<?> prunedTable = tableSupplier.tableFor(TableQueryV4.builder()
				.groupByToAggregators(ImmutableSetMultimap.of(IGroupBy.GRAND_TOTAL,
						FilteredAggregator.builder().aggregator(Aggregator.sum("k1")).build()))
				.build());
		Assertions.assertThat(prunedTable.toString())
				.doesNotContain("join")
				.doesNotContain(productTable)
				.doesNotContain(countryTable);

		// End-to-end: DuckDB query still yields the correct total.
		ITabularView result = cube().execute(CubeQuery.builder().measure(k1Sum.getName()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 234));
	}

	// ── 2. GroupBy middle-level column: product join only ──────────────────

	@Test
	public void testByProductName_includesProductPrunesCountry() {
		initTables();
		insertData();
		forest.addMeasure(k1Sum);

		TableLike<?> prunedTable = tableSupplier.tableFor(v4GroupBy("p.productName"));
		Assertions.assertThat(prunedTable.toString()).contains(productTable).doesNotContain(countryTable);

		ITabularView result =
				cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("p.productName").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("p.productName", "Carotte"), Map.of(k1Sum.getName(), 0L + 123))
				.containsEntry(MapWithNulls.of("p.productName", null), Map.of(k1Sum.getName(), 0L + 234))
				.hasSize(2);
	}

	// ── 3. GroupBy leaf column: both joins required (snowflake transitivity) ──

	@Test
	public void testByCountryName_includesBothJoins() {
		initTables();
		insertData();
		forest.addMeasure(k1Sum);

		TableLike<?> prunedTable = tableSupplier.tableFor(v4GroupBy("c.countryName"));
		Assertions.assertThat(prunedTable.toString()).contains(productTable).contains(countryTable);

		ITabularView result =
				cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("c.countryName").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("c.countryName", "France"), Map.of(k1Sum.getName(), 0L + 123))
				.containsEntry(MapWithNulls.of("c.countryName", null), Map.of(k1Sum.getName(), 0L + 234))
				.hasSize(2);
	}

	// ── 4. Schema introspection still reports every column from the all-joins table ──

	@Test
	public void testGetColumns_reportsAllThreeTables() {
		initTables();

		Assertions.assertThat(table().getColumnTypes())
				.containsEntry("k1", Integer.class)
				.containsEntry("productId", String.class)
				.containsEntry("productName", String.class)
				.containsEntry("countryId", String.class)
				.containsEntry("countryName", String.class)
				.hasSize(5);
	}
}
