/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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

import org.assertj.core.api.Assertions;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.table.sql.AdhocJooqHelper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

public class TestJooqTableSupplierBuilder {
	static {
		AdhocJooqHelper.disableBanners();
	}

	final JooqTableSupplierBuilder snowflakeBuilder = JooqTableSupplierBuilder.builder()
			.dslSupplier(DuckDBHelper.inMemoryDSLSupplier())
			.baseTable(DSL.table("baseTable"))
			.baseTableAlias("base")
			.build();

	@Test
	public void testSnowflake() {
		snowflakeBuilder.leftJoin(j -> j.table(DSL.table("joinedTable")).alias("joined").on("baseA", "joinedA"));

		Table<Record> snowflakeTable = snowflakeBuilder.getSnowflakeTable();

		Assertions.assertThat(snowflakeTable.toString()).isEqualTo("""
				baseTable "base"
				  left outer join joinedTable "joined"
				    on "base"."baseA" = "joined"."joinedA"
								""".trim());

		Assertions.assertThat(snowflakeBuilder.getAliasToOriginal())
				.containsEntry("baseA", "\"base\".\"baseA\"")
				.hasSize(1);
	}

	@Test
	public void testSnowflake_explicitForeignKeyWithDot() {
		snowflakeBuilder.leftJoin(j -> j.table(DSL.table("joinedTable1")).alias("joined1").on("baseA", "joined1A"))
				.leftJoin(j -> j.table(DSL.table("joinedTable2")).alias("joined2").on("joined1.baseA", "joined2A"));

		Table<Record> snowflakeTable = snowflakeBuilder.getSnowflakeTable();

		Assertions.assertThat(snowflakeTable.toString()).isEqualTo("""
				baseTable "base"
				  left outer join joinedTable1 "joined1"
				    on "base"."baseA" = "joined1"."joined1A"
				  left outer join joinedTable2 "joined2"
				    on "joined1"."baseA" = "joined2"."joined2A"
								                """.trim());

		Assertions.assertThat(snowflakeBuilder.getAliasToOriginal())
				.containsEntry("baseA", "\"base\".\"baseA\"")
				.hasSize(1);
	}

	@Test
	public void testSnowflake_fieldWithWithDot() {
		snowflakeBuilder.leftJoin(j -> j.table(DSL.table("joinedTable1")).alias("joined1").on("baseA", "joined1A"))
				.leftJoin(
						j -> j.table(DSL.table("joinedTable2")).alias("joined2").on("\"ill_name.baseA\"", "joined2A"));

		Table<Record> snowflakeTable = snowflakeBuilder.getSnowflakeTable();

		Assertions.assertThat(snowflakeTable.toString()).isEqualTo("""
				baseTable "base"
				  left outer join joinedTable1 "joined1"
				    on "base"."baseA" = "joined1"."joined1A"
				  left outer join joinedTable2 "joined2"
				    on "base"."ill_name.baseA" = "joined2"."joined2A"
								                """.trim());

		Assertions.assertThat(snowflakeBuilder.getAliasToOriginal())
				.containsEntry("baseA", "\"base\".\"baseA\"")
				.containsEntry("ill_name.baseA", "\"base\".\"ill_name.baseA\"")
				.hasSize(2);
	}

	// Weird pathes can be prefixed with `;`
	@Test
	public void testSnowflake_weirdPath() {
		snowflakeBuilder.leftJoin(j -> j.table(DSL.table("joinedTable1")).alias("joined1").on("baseA", "joined1A"))
				.leftJoin(j -> j.table(DSL.table("joinedTable2")).alias("joined2").on(";ill_name.baseA''", "joined2A"));

		Table<Record> snowflakeTable = snowflakeBuilder.getSnowflakeTable();

		Assertions.assertThat(snowflakeTable.toString()).isEqualTo("""
				baseTable "base"
				  left outer join joinedTable1 "joined1"
				    on "base"."baseA" = "joined1"."joined1A"
				  left outer join joinedTable2 "joined2"
				    on ill_name.baseA'' = "joined2"."joined2A"
								                """.trim());

		Assertions.assertThat(snowflakeBuilder.getAliasToOriginal())
				.containsEntry("baseA", "\"base\".\"baseA\"")
				.hasSize(1);
	}

	// Empty-consumer should drop the JOIN cleanly — useful for callers that wrap a JOIN in a runtime
	// condition: when the condition is false, they skip populating the builder and the JOIN simply does not
	// happen, leaving the snowflake table unchanged.
	@Test
	public void testEmptyConsumer_dropsTheJoin() {
		String beforeSql = snowflakeBuilder.getSnowflakeTable().toString();

		// No-op consumer — does not call .table(...), .alias(...), or .on(...).
		snowflakeBuilder.leftJoin(j -> {
		});

		// Snowflake table is unchanged: no JOIN was committed.
		Assertions.assertThat(snowflakeBuilder.getSnowflakeTable().toString()).isEqualTo(beforeSql);
		Assertions.assertThat(snowflakeBuilder.getAliasToOriginal()).isEmpty();
	}

	@Test
	public void testSnowflake_withAlias() {
		snowflakeBuilder.withAlias("aliasBase", "baseA")
				.leftJoin(j -> j.table(DSL.table("joinedTable1"))
						.alias("joined1")
						.on("baseA", "joined1A")
						.withAlias("alias1", "joined1A"))
				.leftJoin(j -> j.table(DSL.table("joinedTable2"))
						.alias("joined2")
						.on(";ill_name.baseA''", "joined2A")
						.withAlias("alias2", "joined2A"));

		Table<Record> snowflakeTable = snowflakeBuilder.getSnowflakeTable();

		Assertions.assertThat(snowflakeTable.toString()).isEqualTo("""
				baseTable "base"
				  left outer join joinedTable1 "joined1"
				    on "base"."baseA" = "joined1"."joined1A"
				  left outer join joinedTable2 "joined2"
				    on ill_name.baseA'' = "joined2"."joined2A"
								                """.trim());

		Assertions.assertThat(snowflakeBuilder.getAliasToOriginal())
				.containsEntry("baseA", "\"base\".\"baseA\"")
				.containsEntry("aliasBase", "\"base\".\"baseA\"")
				.containsEntry("alias1", "\"joined1\".\"joined1A\"")
				.containsEntry("alias2", "\"joined2\".\"joined2A\"")
				.hasSize(4);
	}

	// `from(...)` is purely a convenience that prepends a single alias to unqualified left columns. When the
	// left side of an ON clause needs to mix columns from MULTIPLE earlier tables (typical multi-key join such
	// as `(a.id, b.color) → c`), the caller can omit `from(...)` entirely and pass each left column as a
	// fully-qualified two-part name. `parseOnName` returns multipart names as-is without prepending anything,
	// so `a.id` resolves to `a.id` and `b.color` resolves to `b.color` — both correctly bound to their own
	// owning table even though no single `from(...)` could express the pair.
	@Test
	public void testSnowflake_multiTableLeftSide_noFrom_qualifiesEachLeftColumn() {
		snowflakeBuilder
				// base → a (single-key join, default `from` = base)
				.leftJoin(j -> j.table(DSL.table("table_a")).alias("a").on("a_id", "id"))
				// base → b (single-key join, default `from` = base)
				.leftJoin(j -> j.table(DSL.table("table_b")).alias("b").on("b_id", "id"))
				// (a, b) → c on (a.id = c.a_id AND b.color = c.color) — multi-table left side: NO `from(...)`,
				// each left column is given as the fully-qualified `<alias>.<column>` form.
				.leftJoin(j -> j.table(DSL.table("table_c")).alias("c").on("a.id", "a_id").on("b.color", "color"));

		Table<Record> snowflakeTable = snowflakeBuilder.getSnowflakeTable();

		Assertions.assertThat(snowflakeTable.toString()).isEqualTo("""
				baseTable "base"
				  left outer join table_a "a"
				    on "base"."a_id" = "a"."id"
				  left outer join table_b "b"
				    on "base"."b_id" = "b"."id"
				  left outer join table_c "c"
				    on (
				      "a"."id" = "c"."a_id"
				      and "b"."color" = "c"."color"
				    )
								""".trim());
	}
}
