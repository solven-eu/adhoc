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

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.Table;
import org.jooq.TableLike;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSetMultimap;

import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.query.IGroupBy;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.sql.AdhocJooqHelper;
import eu.solven.adhoc.table.sql.JooqColumnsHelpers;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

/**
 * Pinning tests for the column-alias mechanism in {@link PrunedJoinsJooqTableSupplier}: how
 * {@link JooqJoinBuilder#withAlias(String, String)} (and the equivalent
 * {@link JooqTableSupplierBuilder#withAlias(String, String)} on the parent builder) interacts with the supplier's
 * column→alias index, with multiple joins, with the base table, and across cross-join targets.
 *
 * <p>
 * These tests are intentionally characterization-flavoured: they pin the current resolution rules (e.g. last-wins on
 * alias collision, alias takes precedence over a same-named base column) so future refactors of
 * {@link PrunedJoinsJooqTableSupplier#computeNeededAliases} or the underlying {@code aliasToOriginal} machinery have to
 * flag any behaviour change.
 *
 * @author Benoit Lacelle
 */
public class TestPrunedJoinsJooqTableSupplier_Aliases {
	static {
		AdhocJooqHelper.disableBanners();
	}

	private static PrunedJoinsJooqTableSupplierBuilder newBuilder() {
		return PrunedJoinsJooqTableSupplierBuilder.prunedBuilder()
				.dslSupplier(DuckDBHelper.inMemoryDSLSupplier())
				.baseTable(DSL.table("fact"))
				.baseTableAlias("fact")
				.build()
				// Union of every base-side column referenced across the tests in this file.
				.baseProvidedColumns(Set.of("amount", "country", "id", "a_id", "b_id"));
	}

	private static PrunedJoinsJooqTableSupplier supplier(PrunedJoinsJooqTableSupplierBuilder builder) {
		return PrunedJoinsJooqTableSupplier.builder()
				.schema(builder)
				.columnsResolver(JooqColumnsHelpers.fromJooqFields())
				.build();
	}

	private static TableQueryV4 queryGroupBy(String column, String aggregatedColumn) {
		IGroupBy gb = GroupByColumns.named(column);
		FilteredAggregator agg = FilteredAggregator.builder().aggregator(Aggregator.sum(aggregatedColumn)).build();
		return TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gb, agg)).build();
	}

	/** Returns a VALUES-based jOOQ table whose {@code fields()} report the declared column names. */
	private static Table<?> tableWithFields(String tableName, String... columnNames) {
		return DSL.values(DSL.row(new Object[columnNames.length])).asTable(tableName, columnNames);
	}

	// ── 1. Baseline — alias propagates the join dependency ──────────────────

	@Test
	public void testWithAlias_pullsUnderlyingJoinIntoFrom() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(tableWithFields("dim_a", "a_id", "real_name"))
				.alias("a")
				.from("fact")
				.onSame("a_id")
				.withAlias("display_name", "real_name"));

		// Querying the alias should pull `a` into the FROM, just like querying `real_name` directly would.
		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("display_name", "amount"));

		Assertions.assertThat(pruned.toString()).contains("\"a\"");
	}

	// ── 2. Alias storage — qualification rules ──────────────────────────────

	@Test
	public void testWithAlias_unqualifiedOriginal_qualifiesToLatestJoin() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(tableWithFields("dim_a", "a_id", "real_name"))
				.alias("a")
				.from("fact")
				.onSame("a_id")
				.withAlias("display_name", "real_name"));

		// Storage convention: the unqualified `original` is auto-prefixed with the join's own alias and stored as
		// the JOOQ-escaped two-part Name. This is what `PrunedJoinsJooqTableSupplier` consults on lookup.
		Assertions.assertThat(builder.getAliasToOriginal())
				.containsEntry("display_name", DSL.name("a", "real_name").toString());
	}

	@Test
	public void testWithAlias_qualifiedOriginal_keepsExplicitOwner() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(
				j -> j.table(tableWithFields("dim_a", "a_id", "ignored_a")).alias("a").from("fact").onSame("a_id"))
				.leftJoin(j -> j.table(tableWithFields("dim_b", "b_id", "real_name"))
						.alias("b")
						.from("fact")
						.onSame("b_id")
						.withAlias("display_name", "real_name"));

		// `latestJoin` at registration time is `b` (the join the alias is declared on), so the alias resolves to
		// `"b"."real_name"` — NOT auto-prefixed with `a` even though `a` was registered first.
		Assertions.assertThat(builder.getAliasToOriginal())
				.containsEntry("display_name", DSL.name("b", "real_name").toString());

		// Querying the alias keeps `b` and prunes `a` (which provides nothing the query references).
		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("display_name", "amount"));
		String sql = pruned.toString();
		Assertions.assertThat(sql).contains("\"b\"").doesNotContain("\"a\"");
	}

	// ── 3. Builder-level withAlias (after the join closes) ──────────────────

	@Test
	public void testWithAlias_calledOnBuilder_qualifiesAgainstLatestJoin() {
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(
				j -> j.table(tableWithFields("dim_a", "a_id", "real_name")).alias("a").from("fact").onSame("a_id"));
		// Equivalent to declaring the alias inside the join lambda; uses `latestJoin` (= "a") to qualify.
		builder.withAlias("display_name", "real_name");

		Assertions.assertThat(builder.getAliasToOriginal())
				.containsEntry("display_name", DSL.name("a", "real_name").toString());

		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("display_name", "amount"));
		Assertions.assertThat(pruned.toString()).contains("\"a\"");
	}

	// ── 4. Collisions ────────────────────────────────────────────────────────

	@Test
	public void testWithAlias_collidesWithBaseColumn_aliasWins() {
		// Base provides `country`. The alias `country -> a_country` redirects the lookup to `a`. Today the
		// supplier consults `aliasToOriginal` BEFORE the index, so the alias wins: `a` is pulled in even though
		// the base also offers a `country` column.
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(tableWithFields("dim_a", "a_id", "a_country"))
				.alias("a")
				.from("fact")
				.onSame("a_id")
				.withAlias("country", "a_country"));

		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("country", "amount"));

		// Alias diverts to `a`; the base `country` column is shadowed for the alias key.
		Assertions.assertThat(pruned.toString()).contains("\"a\"");
	}

	@Test
	public void testWithAlias_collidesWithOtherJoinColumn_aliasOwnerWins() {
		// `b` natively provides `name`; `a` declares an alias `name -> a_real`. The alias points the lookup to
		// `a` regardless of `b`'s column. `b` is pruned because its only contribution (`name`) is shadowed by
		// the alias.
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(tableWithFields("dim_a", "a_id", "a_real"))
				.alias("a")
				.from("fact")
				.onSame("a_id")
				.withAlias("name", "a_real"))
				.leftJoin(
						j -> j.table(tableWithFields("dim_b", "b_id", "name")).alias("b").from("fact").onSame("b_id"));

		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("name", "amount"));

		String sql = pruned.toString();
		Assertions.assertThat(sql).contains("\"a\"").doesNotContain("\"b\"");
	}

	// ── 5. Cross-join alias ─────────────────────────────────────────────────

	@Test
	public void testWithAlias_pointsToColumnInDifferentJoin_pullsThatJoin() {
		// Alias declared on `a` but explicitly qualified against `b`. The `latestJoin` at registration is `b`
		// (the second join), but the original is fully-qualified, so `parseOnName` keeps the explicit owner.
		// Querying the alias must pull `b` into the FROM, not `a`.
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(
				j -> j.table(tableWithFields("dim_a", "a_id", "ignored_a")).alias("a").from("fact").onSame("a_id"))
				.leftJoin(j -> j.table(tableWithFields("dim_b", "b_id", "b_real"))
						.alias("b")
						.from("fact")
						.onSame("b_id")
						.withAlias("display_name", "b.b_real"));

		Assertions.assertThat(builder.getAliasToOriginal())
				.containsEntry("display_name", DSL.name("b", "b_real").toString());

		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("display_name", "amount"));
		String sql = pruned.toString();
		Assertions.assertThat(sql).contains("\"b\"").doesNotContain("\"a\"");
	}

	// ── 6. Fallback paths ───────────────────────────────────────────────────

	@Test
	public void testWithAlias_doesNotShadowOriginalColumnName() {
		// Aliasing `display_name -> real_name` does NOT remove `real_name` from the index — querying the
		// underlying column directly still resolves through the index, independent of the alias.
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(tableWithFields("dim_a", "a_id", "real_name"))
				.alias("a")
				.from("fact")
				.onSame("a_id")
				.withAlias("display_name", "real_name"));

		// Query references the underlying column, not the alias — index lookup, alias map ignored.
		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("real_name", "amount"));
		Assertions.assertThat(pruned.toString()).contains("\"a\"");
	}

	// ── 7. Same-alias-twice — last write wins ───────────────────────────────

	@Test
	public void testWithAlias_sameAliasTwice_lastWins() {
		// Two joins each register `display_name`. The supplier's `aliasToOriginal` map uses `Map.put` (not
		// `putIfAbsent`), so the SECOND registration overwrites the first. The query for `display_name` pulls
		// in `b` (the last writer) and prunes `a`.
		PrunedJoinsJooqTableSupplierBuilder builder = newBuilder();
		builder.leftJoin(j -> j.table(tableWithFields("dim_a", "a_id", "a_real"))
				.alias("a")
				.from("fact")
				.onSame("a_id")
				.withAlias("display_name", "a_real"))
				.leftJoin(j -> j.table(tableWithFields("dim_b", "b_id", "b_real"))
						.alias("b")
						.from("fact")
						.onSame("b_id")
						.withAlias("display_name", "b_real"));

		Assertions.assertThat(builder.getAliasToOriginal())
				.containsEntry("display_name", DSL.name("b", "b_real").toString());

		TableLike<?> pruned = supplier(builder).tableFor(queryGroupBy("display_name", "amount"));
		String sql = pruned.toString();
		Assertions.assertThat(sql).contains("\"b\"").doesNotContain("\"a\"");
	}
}
