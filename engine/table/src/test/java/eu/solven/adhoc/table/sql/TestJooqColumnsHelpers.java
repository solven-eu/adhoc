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
package eu.solven.adhoc.table.sql;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

/**
 * Verifies the {@link IJooqColumnsResolver} factories exposed by {@link JooqColumnsHelpers}.
 *
 * @author Benoit Lacelle
 */
public class TestJooqColumnsHelpers {
	static {
		AdhocJooqHelper.disableBanners();
	}

	@Test
	public void testPredefinedSql_executesCallerProvidedSql() {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();
		String someTable = "probed_table";

		// Set up a table with two columns so the probe has something concrete to return.
		dslSupplier.getDSLContext()
				.createTableIfNotExists(someTable)
				.column("col_a", SQLDataType.INTEGER)
				.column("col_b", SQLDataType.VARCHAR)
				.execute();

		// User-supplied SQL — exactly the kind a Redshift-as-PG8 caller would write to dodge JOOQ's renderer.
		// DuckDB stores the table in `memory.main` so we mirror that hierarchy in the SQL.
		IJooqColumnsResolver resolver = JooqColumnsHelpers
				.predefinedSql(table -> "SELECT * FROM \"memory\".\"main\".\"%s\" LIMIT 0".formatted(someTable));

		List<Field<?>> fields = resolver.columnsOf(dslSupplier, DSL.table(DSL.name(someTable)));

		Assertions.assertThat(fields).extracting(Field::getName).containsExactly("col_a", "col_b");
	}

	@Test
	public void testPredefinedSql_passesTableLikeToBuilder() {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();
		String someTable = "named_table";

		dslSupplier.getDSLContext().createTableIfNotExists(someTable).column("only_col", SQLDataType.INTEGER).execute();

		// Builder receives the TableLike — proving the function is fed the queried table, not a hard-coded name.
		IJooqColumnsResolver resolver = JooqColumnsHelpers.predefinedSql(table -> {
			Assertions.assertThat(table.toString()).contains(someTable);
			return "SELECT * FROM \"memory\".\"main\".\"%s\" LIMIT 0".formatted(someTable);
		});

		List<Field<?>> fields = resolver.columnsOf(dslSupplier, DSL.table(DSL.name(someTable)));

		Assertions.assertThat(fields).extracting(Field::getName).containsExactly("only_col");
	}
}
