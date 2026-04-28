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
package eu.solven.adhoc.table.sql.join;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.Condition;
import org.jooq.Table;
import org.jooq.TableLike;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSetMultimap;

import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.sql.AdhocJooqHelper;
import eu.solven.adhoc.table.sql.JooqColumnsHelpers;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

public class TestPrunedJoinsJooqTableSupplierBuilder {
	static {
		AdhocJooqHelper.disableBanners();
	}

	/** Convenience builder seeded with a `fact` base table. */
	private PrunedJoinsJooqTableSupplierBuilder newBuilder() {
		return PrunedJoinsJooqTableSupplierBuilder.prunedBuilder()
				.baseTable(DSL.table("fact"))
				.baseTableAlias("fact")
				.build();
	}

	/** Default-resolver supplier bound to {@code builder}. */
	private static PrunedJoinsJooqTableSupplier supplier(PrunedJoinsJooqTableSupplierBuilder builder) {
		return PrunedJoinsJooqTableSupplier.builder()
				.schema(builder)
				.dslSupplier(DuckDBHelper.inMemoryDSLSupplier())
				.columnsResolver(JooqColumnsHelpers.fromJooqFields())
				.build();
	}

	/** Convenience: build a V4 grouped by {@code col} with a single {@code SUM(m)} aggregator and no filter. */
	private static TableQueryV4 queryGroupBy(String column, String aggregatedColumn) {
		IGroupBy gb = GroupByColumns.named(column);
		FilteredAggregator agg = FilteredAggregator.builder().aggregator(Aggregator.sum(aggregatedColumn)).build();
		return TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gb, agg)).build();
	}

	// ── 1. Base-only query emits zero joins ──────────────────────────────────

	@Test
	public void testNoJoin_whenOnlyBaseColumnsReferenced() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(DSL.table("dim_a"))
				.alias("a")
				.on("a_id", "id")
				.providedColumns(Set.of("a_name", "a_code")));

		// Query references only columns owned by the base table (or not owned by any join).
		TableQueryV4 q = queryGroupBy("region", "amount");

		TableLike<?> pruned = supplier(builder).tableFor(q);

		// FROM should be just the base table — no joins. jOOQ collapses `fact "fact"` (name == alias) to `"fact"`.
		Assertions.assertThat(pruned.toString()).isEqualTo("\"fact\"");
	}

	// ── 2. Leaf-column query includes full ancestor chain ────────────────────

	@Test
	public void testChain_includesAllAncestorsWhenLeafIsNeeded() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(DSL.table("dim_a")).alias("a").on("a_id", "id").providedColumns(Set.of("a_name")))
				.leftJoin(j -> j.table(DSL.table("dim_a_sub"))
						.alias("asub")
						.from("a")
						.on("sub_id", "id")
						.providedColumns(Set.of("sub_label")));

		// Referencing a leaf-level column should pull in both joins (a → asub).
		TableQueryV4 q = queryGroupBy("sub_label", "amount");

		TableLike<?> pruned = supplier(builder).tableFor(q);

		Assertions.assertThat(pruned.toString()).contains("dim_a", "dim_a_sub", "\"a\"", "\"asub\"");
	}

	// ── 3. Two independent arms pruned independently ─────────────────────────

	@Test
	public void testIndependentArms_prunedIndependently() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(DSL.table("dim_a")).alias("a").on("a_id", "id").providedColumns(Set.of("a_name")))
				.leftJoin(
						j -> j.table(DSL.table("dim_b")).alias("b").on("b_id", "id").providedColumns(Set.of("b_name")));

		// Query touches only the `b` arm.
		TableQueryV4 q = queryGroupBy("b_name", "amount");

		TableLike<?> pruned = supplier(builder).tableFor(q);

		String sql = pruned.toString();
		Assertions.assertThat(sql).contains("dim_b").doesNotContain("dim_a");
	}

	// ── 4. Explicit providedColumns override wins ────────────────────────────

	@Test
	public void testExplicitProvidedColumns_isAuthoritative() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		// The override advertises only `a_name`. Any other column is treated as unknown → assumed to belong to the
		// base (so the join is NOT pulled in).
		builder.leftJoin(
				j -> j.table(DSL.table("dim_a")).alias("a").on("a_id", "id").providedColumns(Set.of("a_name")));

		// Index carries both the unqualified `a_name` and the qualified `a.a_name` → "a".
		Assertions.assertThat(supplier(builder).getColumnToAliasSnapshot())
				.containsEntry("a_name", "a")
				.containsEntry("a.a_name", "a")
				.hasSize(2);

		// Query referencing the advertised column → join included.
		TableLike<?> advertisedHit = supplier(builder).tableFor(queryGroupBy("a_name", "amount"));
		Assertions.assertThat(advertisedHit.toString()).contains("dim_a");
	}

	// ── 5. purgeColumnCache triggers re-probe ────────────────────────────────

	@Test
	public void testPurgeColumnCache_rebuildsIndex() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(
				j -> j.table(DSL.table("dim_a")).alias("a").on("a_id", "id").providedColumns(Set.of("a_name")));

		PrunedJoinsJooqTableSupplier supplier = supplier(builder);

		// First call populates the index.
		Map<String, String> firstSnapshot = supplier.getColumnToAliasSnapshot();
		Assertions.assertThat(firstSnapshot).containsEntry("a_name", "a");

		supplier.invalidateAll();

		// After purge, the index is recomputed; content is the same since nothing changed.
		Map<String, String> secondSnapshot = supplier.getColumnToAliasSnapshot();
		Assertions.assertThat(secondSnapshot).isEqualTo(firstSnapshot);
	}

	// ── 6. Needed-alias cache reuse across equivalent queries ────────────────

	@Test
	public void testNeededAliasCache_reusedAcrossEquivalentQueries() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(
				j -> j.table(DSL.table("dim_a")).alias("a").on("a_id", "id").providedColumns(Set.of("a_name")));

		TableQueryV4 q1 = queryGroupBy("a_name", "amount");
		TableQueryV4 q2 = queryGroupBy("a_name", "amount");

		TableLike<?> firstPruned = supplier(builder).tableFor(q1);
		TableLike<?> secondPruned = supplier(builder).tableFor(q2);

		// Two separate Table<Record> instances (we don't cache the table itself), but identical SQL.
		Assertions.assertThat(secondPruned.toString()).isEqualTo(firstPruned.toString());
	}

	// ── 7. Non-prunable joins (low-level leftJoinConditions) always forced ──

	@Test
	public void testLeftJoinConditions_isNonPrunable() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		// Declare via the low-level API — the pruning builder can't know which columns this join provides, so it
		// records a non-prunable (always-included) node.
		Table<?> mandatory = DSL.table("mandatory").as("m");
		Condition on = DSL.field(DSL.name("fact", "m_id")).eq(DSL.field(DSL.name("m", "id")));
		builder.leftJoinConditions(mandatory, List.of(on));

		// Query touches only base columns → normally no joins would be needed, but the non-prunable node must remain.
		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("region", "amount"));

		Assertions.assertThat(pruned.toString()).contains("mandatory");
	}

	// ── 8. Filter columns participate in the needed-set ──────────────────────

	@Test
	public void testFilterColumns_triggerTheirJoin() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(
				j -> j.table(DSL.table("dim_a")).alias("a").on("a_id", "id").providedColumns(Set.of("a_country")));

		// GROUP BY only a base column, but FILTER on a joined column → the join must still be included.
		IGroupBy gb = GroupByColumns.named("region");
		FilteredAggregator agg = FilteredAggregator.builder().aggregator(Aggregator.sum("amount")).build();
		TableQueryV4 q = TableQueryV4.builder()
				.groupByToAggregators(ImmutableSetMultimap.of(gb, agg))
				.filter(ColumnFilter.matchEq("a_country", "FR"))
				.build();

		TableLike<?> pruned = supplier(builder).tableFor(q);
		Assertions.assertThat(pruned.toString()).contains("dim_a");
	}

	// ── 9. getSnowflakeTable returns the all-joins table ─────────────────────

	@Test
	public void testGetSnowflakeTable_containsEveryJoin() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(DSL.table("dim_a")).alias("a").on("a_id", "id").providedColumns(Set.of("a_name")))
				.leftJoin(
						j -> j.table(DSL.table("dim_b")).alias("b").on("b_id", "id").providedColumns(Set.of("b_name")));

		String full = builder.getSnowflakeTable().toString();
		Assertions.assertThat(full).contains("dim_a", "dim_b");
	}

	// ── Derived-column path (no explicit `providedColumns`) ─────────────────
	//
	// The following tests exercise the default path where the builder probes each JoinNode's
	// `joinedTable.fields()` to populate the column→alias index. To get a jOOQ `TableLike` with declared fields
	// (plain `DSL.table("x")` yields an empty `fields()` array), these tests use a VALUES-based derived table
	// built via `DSL.values(...).asTable(alias, fieldNames...)`.

	/** Returns a VALUES-based jOOQ table whose {@code fields()} report the declared column names. */
	private static Table<?> tableWithFields(String tableName, String... columnNames) {
		return DSL.values(DSL.row(new Object[columnNames.length])).asTable(tableName, columnNames);
	}

	@Test
	public void testDerivedColumns_populateIndexFromFieldStream() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		// 4-arg leftJoin — no providedColumns — derived index must come from dim_a.fields().
		builder.leftJoin(
				j -> j.table(tableWithFields("dim_a", "a_id", "a_name")).alias("a").from("fact").onSame("a_id"));

		Assertions.assertThat(supplier(builder).getColumnToAliasSnapshot())
				.containsEntry("a_id", "a")
				.containsEntry("a_name", "a");
	}

	@Test
	public void testDerivedColumns_joinIncludedWhenDerivedColumnReferenced() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(
				j -> j.table(tableWithFields("dim_a", "a_id", "a_name")).alias("a").from("fact").onSame("a_id"));

		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("a_name", "amount"));

		// The derived column `a_name` should pull the `a` join in.
		Assertions.assertThat(pruned.toString()).contains("\"a\"");
	}

	@Test
	public void testDerivedColumns_joinPrunedWhenOnlyBaseColumnsReferenced() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(
				j -> j.table(tableWithFields("dim_a", "a_id", "a_name")).alias("a").from("fact").onSame("a_id"));

		// Query references only `region` (base column) — the `a` join is not needed and must be pruned.
		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("region", "amount"));

		Assertions.assertThat(pruned.toString()).isEqualTo("\"fact\"");
	}

	@Test
	public void testDerivedColumns_snowflakeChainViaDerived() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		// Two-level chain, both joins use the derived path (no explicit providedColumns).
		builder.leftJoin(
				j -> j.table(tableWithFields("dim_a", "a_id", "a_name")).alias("a").from("fact").onSame("a_id"))
				.leftJoin(j -> j.table(tableWithFields("dim_a_sub", "sub_id", "sub_label"))
						.alias("asub")
						.from("a")
						.onSame("sub_id"));

		// Referencing the leaf column must pull in both joins via ancestor closure.
		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("sub_label", "amount"));

		Assertions.assertThat(pruned.toString()).contains("\"a\"", "\"asub\"");
	}

	@Test
	public void testDerivedColumns_mixedWithExplicitOverride() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		// `a` uses derived columns.
		builder.leftJoin(
				j -> j.table(tableWithFields("dim_a", "a_id", "a_name")).alias("a").from("fact").onSame("a_id"));
		// `b` declares an explicit override — its `.fields()` would be ignored anyway since DSL.table() has none.
		builder.leftJoin(
				j -> j.table(DSL.table("dim_b")).alias("b").on("b_id", "id").providedColumns(Set.of("b_country")));

		Assertions.assertThat(supplier(builder).getColumnToAliasSnapshot())
				.containsEntry("a_name", "a")
				.containsEntry("b_country", "b");

		// Query `a_name` (derived) + `b_country` (explicit) → both joins included.
		IGroupBy gbA = GroupByColumns.named("a_name");
		IGroupBy gbB = GroupByColumns.named("b_country");
		FilteredAggregator agg = FilteredAggregator.builder().aggregator(Aggregator.sum("amount")).build();
		TableQueryV4 q =
				TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gbA, agg, gbB, agg)).build();

		TableLike<?> pruned = supplier(builder).tableFor(q);
		Assertions.assertThat(pruned.toString()).contains("\"a\"", "\"b\"");
	}

	@Test
	public void testDerivedColumns_emptyFieldsOnPlainTable_fallsBackToBase() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		// Plain DSL.table("dim_a") has an empty .fields() array → derived index stays empty for this join. No
		// explicit override was given. Any column reference will be treated as "unknown → base" and the join is
		// not pulled in. This matches the documented contract for the *raw* supplier: if derivation cannot find
		// columns, the caller must either supply providedColumns OR go through `JooqTableWrapperParameters`,
		// which auto-upgrades the resolver to a DB probe — see `JooqTableWrapperParametersBuilder.tableSupplier`.
		builder.leftJoin(j -> j.table(DSL.table("dim_a")).alias("a").from("fact").on("a_id", "id"));

		Assertions.assertThat(supplier(builder).getColumnToAliasSnapshot()).isEmpty();

		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("a_name", "amount"));
		// `a_name` is unknown → treated as base column → `a` join is pruned.
		Assertions.assertThat(pruned.toString()).isEqualTo("\"fact\"");
	}

	@Test
	public void testPrunableFalse_forcesJoinIntoEveryQuery() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		// Even though the join advertises `a_name` (so a referenced-column query would NOT pull it in by itself),
		// `.prunable(false)` opts it out of pruning — the join must appear in every materialised FROM clause.
		builder.leftJoin(j -> j.table(DSL.table("dim_a"))
				.alias("a")
				.on("a_id", "id")
				.providedColumns(Set.of("a_name"))
				.prunable(false));

		// Query references nothing the join provides → without the flag, the join would be pruned.
		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("region", "amount"));
		Assertions.assertThat(pruned.toString()).contains("dim_a", "\"a\"");
	}
}
