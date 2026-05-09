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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.IJooqColumnsResolver;
import eu.solven.adhoc.table.sql.JooqColumnsHelpers;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqTableSupplier;
import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqTableSupplierBuilder;
import eu.solven.adhoc.table.transcoder.MapTableAliaser;

/**
 * End-to-end test for {@link PrunedJoinsJooqTableSupplier}'s column-alias mechanism against a real DuckDB backend, with
 * a fixture deliberately built to expose case-sensitivity quirks: the same logical column name lives in two joined
 * tables, but with different casing — {@code b.country} (lowercase) and {@code c.Country} (capitalized) — and a builder
 * alias {@code country → c.Country} is declared so callers always see the capitalized version.
 *
 * <p>
 * The interesting axes:
 * <ul>
 * <li>jOOQ quotes identifiers by default; DuckDB folds unquoted identifiers but preserves quoted ones, so the lowercase
 * / capitalized columns coexist without the engine collapsing them.</li>
 * <li>The supplier's {@code aliasToOriginal} stores the canonical jOOQ-escaped form ({@code "c"."Country"}); the
 * prune-time lookup must hit the same form in the index.</li>
 * <li>If the alias mechanism is wired only into the prune path but not into the cube-side query rendering, querying the
 * alias will fail in surprising ways — these tests will surface that.</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
public class TestDagTableQuery_DuckDb_PrunedJoins_Aliases extends ATestDagDuckDb implements IAdhocTestConstants {

	String factTable = "fact_alias";
	String bTable = "dim_b";
	String cTable = "dim_c";

	IJooqColumnsResolver columnsResolver = JooqColumnsHelpers.dbProbe();

	PrunedJoinsJooqTableSupplierBuilder snowflakeBuilder = PrunedJoinsJooqTableSupplierBuilder.prunedBuilder()
			.baseTable(DSL.table(DSL.name(factTable)))
			.baseTableAlias("f")
			.dslSupplier(dslSupplier)
			.build()
			// fact → b: b.country (lowercase). Star: defaults to base `f`.
			.leftJoin(j -> j.table(DSL.table(DSL.name(bTable))).alias("b").on("b_id", "id"))
			// fact → c: c.Country (capitalized). Builder-side alias `country → c.Country` redirects any caller
			// referencing `country` to c's capitalized column.
			.leftJoin(j -> j.table(DSL.table(DSL.name(cTable)))
					.alias("c")
					.on("c_id", "id")
					.withAlias("country", "Country")
					// Column name with a space — jOOQ keeps the quotes around it in the qualified form
					// `"c"."country group"`. Exercises the parsing site at CubeWrapper.getColumns
					// (`tableName.substring(lastDot + 1)`) which today returns `"country group"` (leading quote
					// preserved) and fails to look the column up in the table's column metadata map.
					.withAlias("country_group", "country group"));

	PrunedJoinsJooqTableSupplier tableSupplier =
			PrunedJoinsJooqTableSupplier.builder().schema(snowflakeBuilder).columnsResolver(columnsResolver).build();

	@Override
	public ITableWrapper makeTable() {
		JooqTableWrapperParameters params = DuckDBHelper.parametersBuilder(dslSupplier)
				.tableSupplier(tableSupplier)
				.columnsResolver(columnsResolver)
				.build();
		return new JooqTableWrapper(factTable, params);
	}

	@Override
	public CubeWrapper.CubeWrapperBuilder makeCube() {
		// Wire the supplier's aliasToOriginal map into the cube's ColumnsManager. Without this, the cube's
		// default `IdentityImplicitAliaser` ignores supplier-side aliases — `country_group` would reach the
		// SQL renderer untranslated and DuckDB would report "Referenced column country_group not found".
		// TODO Auto-wire this in CubeWrapper when the table is a JooqTableWrapper backed by a supplier with
		// `getAliasToOriginal()` non-empty — today every caller has to plumb it manually.
		MapTableAliaser aliaser =
				MapTableAliaser.builder().aliasToOriginals(snowflakeBuilder.getAliasToOriginal()).build();
		return super.makeCube().columnsManager(ColumnsManager.builder().aliaser(aliaser).build());
	}

	@BeforeEach
	public void initAndInsert() {
		dsl.createTableIfNotExists(factTable)
				.column("k1", SQLDataType.INTEGER)
				.column("b_id", SQLDataType.VARCHAR)
				.column("c_id", SQLDataType.VARCHAR)
				.execute();
		dsl.createTableIfNotExists(bTable)
				.column("id", SQLDataType.VARCHAR)
				.column("country", SQLDataType.VARCHAR)
				.execute();
		// `Country` is quoted on create so DuckDB keeps the capital — without quoting, the engine would fold
		// the identifier to lowercase and we would collide with `b.country` at the engine level. The
		// `country group` column has a space — exercises identifiers that need quoting throughout the stack.
		dsl.createTableIfNotExists(cTable)
				.column("id", SQLDataType.VARCHAR)
				.column(DSL.quotedName("Country"), SQLDataType.VARCHAR)
				.column(DSL.quotedName("country group"), SQLDataType.VARCHAR)
				.execute();

		dsl.insertInto(DSL.table(factTable), DSL.field("k1"), DSL.field("b_id"), DSL.field("c_id"))
				.values(100, "B1", "C1")
				.execute();
		dsl.insertInto(DSL.table(factTable), DSL.field("k1"), DSL.field("b_id"), DSL.field("c_id"))
				.values(200, "B1", "C2")
				.execute();

		dsl.insertInto(DSL.table(bTable), DSL.field("id"), DSL.field("country"))
				.values("B1", "from_b_lowercase")
				.execute();
		dsl.insertInto(DSL.table(cTable),
				DSL.field("id"),
				DSL.field(DSL.quotedName("Country")),
				DSL.field(DSL.quotedName("country group"))).values("C1", "from_c_C1", "EU").execute();
		dsl.insertInto(DSL.table(cTable),
				DSL.field("id"),
				DSL.field(DSL.quotedName("Country")),
				DSL.field(DSL.quotedName("country group"))).values("C2", "from_c_C2", "AS").execute();

		forest.addMeasure(k1Sum);
	}

	// ── Storage-level pin: the alias resolves to the capitalized column ────────────────────────────────────

	@Test
	public void testAliasMap_capturesCapitalizedTarget() {
		// `withAlias("country", "Country")` declared inside `c`'s join lambda → unqualified `Country` is auto-
		// prefixed with `latestJoin` (= "c") and stored as the JOOQ-escaped two-part Name. This is the form the
		// supplier's lookup compares against the column→alias index.
		Assertions.assertThat(snowflakeBuilder.getAliasToOriginal())
				.containsEntry("country", DSL.name("c", "Country").toString());
	}

	// ── Prune-time decision: the alias pulls in `c`, prunes `b` ────────────────────────────────────────────

	@Test
	public void testPruneByAlias_routesToCAndPrunesB() {
		// Querying via the alias must keep `c` (the alias's target) in the FROM and prune `b` — even though
		// `b` natively offers a lowercase `country` column that, without the alias, would be the natural owner.
		String prunedSql = tableSupplier.tableFor(v4GroupBy("country")).toString();
		Assertions.assertThat(prunedSql).contains("\"c\"").doesNotContain("\"b\"");
	}

	@Test
	public void testGetColumns() {
		// The alias `country -> c.Country` claims the bare name `country`. Since `b.country` (lowercase, on `b`)
		// already exists with that bare name, the column list re-exposes b's column under its qualified form
		// `b.country` — so the bare `country` name unambiguously means "c.Country" (via the alias) and the
		// shadowed b column stays reachable as `b.country`. The `country group` column (with space) is the
		// target of `withAlias("country_group", "country group")` — verifies that quoted identifiers flow
		// through the alias pipeline correctly (no leading-quote leakage from the `lastDot` parsing site).
		Assertions.assertThat(table().getColumnsAsMap())
				.containsOnlyKeys("k1", "b_id", "c_id", "id", "b.country", "Country", "country group")
				.hasEntrySatisfying("b.country", c -> {
					Assertions.assertThat(c.getAliases()).isEmpty();
				})
				.hasEntrySatisfying("Country", c -> {
					Assertions.assertThat(c.getAliases()).containsExactly("country");
				})
				.hasEntrySatisfying("country group", c -> {
					Assertions.assertThat(c.getAliases()).containsExactly("country_group");
				});
	}

	@Test
	public void testQueryByAlias_columnWithSpace() {
		ITabularView result =
				cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("country_group").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("country_group", "EU"), Map.of(k1Sum.getName(), 0L + 100))
				.containsEntry(Map.of("country_group", "AS"), Map.of(k1Sum.getName(), 0L + 200))
				.hasSize(2);
	}

	// ── End-to-end execution: rows actually come from `c.Country` ──────────────────────────────────────────

	@Test
	public void testQueryByAlias_returnsCapitalizedRows() {
		ITabularView result =
				cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("country").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// Row k1=100 has c_id=C1 → c.Country='from_c_C1'; row k1=200 has c_id=C2 → c.Country='from_c_C2'. The
		// alias must route `country` to c's capitalized column, NOT to b's lowercase column (which would have
		// reported 'from_b_lowercase' for both rows).
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("country", "from_c_C1"), Map.of(k1Sum.getName(), 0L + 100))
				.containsEntry(Map.of("country", "from_c_C2"), Map.of(k1Sum.getName(), 0L + 200))
				.hasSize(2);
	}

	// ── Direct access to b's lowercase column still works (alias does not shadow the index) ────────────────

	@Test
	public void testQueryByBQualifiedColumn_returnsLowercaseRows() {
		ITabularView result =
				cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("b.country").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// Both fact rows share b_id=B1 → both aggregate into the same b.country slice.
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("b.country", "from_b_lowercase"), Map.of(k1Sum.getName(), 0L + 100 + 200))
				.hasSize(1);
	}

	// ── Direct access to c's capitalized column via bare-dotted form ───────────────────────────────────────

	@Test
	public void testQueryByCQualifiedColumn_returnsCapitalizedRows() {
		ITabularView result =
				cube().execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("c.Country").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("c.Country", "from_c_C1"), Map.of(k1Sum.getName(), 0L + 100))
				.containsEntry(Map.of("c.Country", "from_c_C2"), Map.of(k1Sum.getName(), 0L + 200))
				.hasSize(2);
	}

	/** Builds a minimal V4 query used only to inspect the pruning decision (not executed). */
	private static eu.solven.adhoc.query.table.TableQueryV4 v4GroupBy(String column) {
		return eu.solven.adhoc.query.table.TableQueryV4.builder()
				.groupByToAggregators(com.google.common.collect.ImmutableSetMultimap.of(
						eu.solven.adhoc.model.query.groupby.GroupByColumns.named(column),
						eu.solven.adhoc.query.table.FilteredAggregator.builder()
								.aggregator(eu.solven.adhoc.model.measure.Aggregator.sum("k1"))
								.build()))
				.build();
	}
}
