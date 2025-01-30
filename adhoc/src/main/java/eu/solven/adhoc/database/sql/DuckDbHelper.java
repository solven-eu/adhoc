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
package eu.solven.adhoc.database.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.duckdb.DuckDBConnection;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import lombok.NonNull;

/**
 * Helps working with DuckDB.
 */
public class DuckDbHelper {
	protected DuckDbHelper() {
		// hidden
	}

	/**
	 * This {@link DuckDBConnection} should be `.duplicate` in case of multi-threaded access.
	 *
	 * @return a {@link DuckDBConnection} to an new DuckDB InMemory instance.
	 */
	// https://duckdb.org/docs/api/java.html
	public static DuckDBConnection makeFreshInMemoryDb() {
		try {
			return (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		} catch (SQLException e) {
			throw new IllegalStateException("Issue opening an InMemory DuchDB", e);
		}
	}

	/**
	 *
	 * @return a {@link DSLSupplier} based on provided {@link SQLDialect}
	 */
	public static @NonNull DSLSupplier inMemoryDSLSupplier() {
		DuckDBConnection duckDbConnection = DuckDbHelper.makeFreshInMemoryDb();
		return () -> {
			Connection duplicated;
			try {
				duplicated = duckDbConnection.duplicate();
			} catch (SQLException e) {
				throw new IllegalStateException("Issue duplicating an InMemory DuckDB connection", e);
			}
			return DSL.using(duplicated);
		};
	}
}
