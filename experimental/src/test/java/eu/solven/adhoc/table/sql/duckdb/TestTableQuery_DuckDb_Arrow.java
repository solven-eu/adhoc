/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.sql.duckdb;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.ATestTableQuery_DB;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;

public class TestTableQuery_DuckDb_Arrow extends ATestTableQuery_DB {

	@Override
	public IDSLSupplier makeDSLSupplier() {
		return DuckDBHelper.inMemoryDSLSupplier();
	}

	@Override
	protected @NonNull Class<? extends Throwable> expectedExceptionClassForMissing() {
		return IllegalArgumentException.class;
	}

	@Override
	public ITableWrapper makeTable() {
		JooqTableWrapperParameters jooqParameters =
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName).build();
		return new DuckDBTableWrapper(tableName, DuckDBTableWrapperParameters.builder().base(jooqParameters).build());
	}

	// No DataAccessException as we do not query through Jooq
	@Test
	public void testTableDoesNotExists() {
		Assertions.assertThatThrownBy(() -> table().streamSlices(qK1).toList())
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining(
						"Caused by: java.sql.SQLException: Catalog Error: Table with name someTableName does not exist!");
	}

	// No DataAccessException as we do not query through Jooq
	@Test
	public void test_GroupByUnknown() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.DOUBLE).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values(12.34).execute();

		Assertions
				.assertThatThrownBy(() -> table()
						.streamSlices(qK1.toBuilder().groupBy(GroupByColumns.named("unknownColumn")).build())
						.toList())
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("Caused by: java.sql.SQLException: "
						+ "Binder Error: Referenced column \"unknownColumn\" not found in FROM clause!");
	}

	// No DataAccessException as we do not query through Jooq
	@Test
	public void test_sumOverVarChar() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.VARCHAR).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values("someKey").execute();

		Assertions.assertThatThrownBy(() -> table().streamSlices(qK1).toList())
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("java.sql.SQLException: Binder Error:"
						+ " No function matches the given name and argument types 'sum(VARCHAR)'."
						+ " You might need to add explicit type casts");
	}

}
