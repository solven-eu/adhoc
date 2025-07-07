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
package eu.solven.adhoc.data.cell;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.duckdb.DuckDBConnection;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

// https://stackoverflow.com/questions/79692856/jooq-dynamic-aggregated-types
public class TestDuckDbJooqAggregate {
	@Disabled
	@Test
	public void testAggregate() throws SQLException {
		DuckDBConnection c = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		DSLContext dslContext = DSL.using(c, SQLDialect.DUCKDB);

		String tableName = "someTable";

		dslContext.createTable(tableName).column("kI", SQLDataType.INTEGER).column("kD", SQLDataType.DOUBLE).execute();
		dslContext.insertInto(DSL.table(tableName), DSL.field("kI"), DSL.field("kD")).values(123, 12.34).execute();
		dslContext.insertInto(DSL.table(tableName), DSL.field("kI"), DSL.field("kD")).values(234, 23.45).execute();

		try (Statement statement = c.createStatement()) {
			statement.execute("SELECT SUM(kI), SUM(kD) FROM someTable");
			ResultSet resultSet = statement.getResultSet();
			if (resultSet.next()) {
				// Prints `class java.math.BigInteger`
				System.out.println(resultSet.getObject(1).getClass());
				// Prints `class Double`
				System.out.println(resultSet.getObject(2).getClass());
			}
		}

		// We need to proceed over `kI` and `kD` without knowing their type before hand
		onFieldName(dslContext, tableName, "kI");
		onFieldName(dslContext, tableName, "kD");

		onFieldNameCoerceBigInteger(dslContext, tableName, "kI");
		onFieldNameCoerceBigInteger(dslContext, tableName, "kD");

		// Fails with `Not supported by dialect : Type class java.lang.Number is not supported in dialect null`
		onFieldNameCoerceNumber(dslContext, tableName, "kI");
		onFieldNameCoerceNumber(dslContext, tableName, "kD");
	}

	private void onFieldName(DSLContext dslContext, String tableName, String fieldName) {
		Field<Number> field = (Field) DSL.field(fieldName);
		SelectJoinStep<Record1<BigDecimal>> queryInteger = dslContext.select(DSL.sum(field)).from(DSL.table(tableName));

		queryInteger.stream().findAny().ifPresent(row -> {
			// prints `class java.math.BigDecimal`
			System.out.println(row.get(0).getClass());
		});
	}

	private void onFieldNameCoerceBigInteger(DSLContext dslContext, String tableName, String fieldName) {
		Field<Number> field = (Field) DSL.field(fieldName);
		SelectJoinStep<Record1<BigInteger>> queryInteger =
				dslContext.select(DSL.sum(field).coerce(BigInteger.class)).from(DSL.table(tableName));

		queryInteger.stream().findAny().ifPresent(row -> {
			// prints `class java.math.BigDecimal`
			System.out.println(row.get(0).getClass());
		});
	}

	private void onFieldNameCoerceNumber(DSLContext dslContext, String tableName, String fieldName) {
		Field<Number> field = (Field) DSL.field(fieldName);
		SelectJoinStep<Record1<Number>> queryInteger =
				dslContext.select(DSL.sum(field).coerce(Number.class)).from(DSL.table(tableName));

		queryInteger.stream().findAny().ifPresent(row -> {
			// prints `class java.math.BigDecimal`
			System.out.println(row.get(0).getClass());
		});
	}
}
