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
package eu.solven.adhoc.table.sql.duckdb;

import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Wrapper;

import javax.sql.DataSource;

import org.assertj.core.api.Assertions;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDuckDBDataSource {
	DuckDBDataSource dataSource = new DuckDBDataSource(DuckDbHelper.makeFreshInMemoryDb());

	@Test
	public void testCloseCurrentThreadReUse() throws SQLException {
		Connection initialC;
		try (Connection c = dataSource.getConnection()) {
			initialC = c;
			DuckDBConnection duckdbC = ((Wrapper) c).unwrap(DuckDBConnection.class);

			try (Statement s = duckdbC.createStatement()) {
				s.execute("CREATE TABLE someTableName (color VARCHAR, value FLOAT);");
			}
		}

		Connection laterC;
		try (Connection c = dataSource.getConnection()) {
			laterC = c;
			DuckDBConnection duckdbC = ((Wrapper) c).unwrap(DuckDBConnection.class);

			try (Statement s = duckdbC.createStatement()) {
				s.execute("CREATE TABLE someTableName2 (color2 VARCHAR, value2 FLOAT);");
			}
		}

		Assertions.assertThat(initialC).isNotSameAs(laterC);
	}

	@Test
	public void testImbricated() throws SQLException {
		try (Connection outerC = dataSource.getConnection()) {
			DuckDBConnection outerDuckDBC = ((Wrapper) outerC).unwrap(DuckDBConnection.class);

			try (Connection innerC = dataSource.getConnection()) {
				DuckDBConnection innerDuckdbC = ((Wrapper) innerC).unwrap(DuckDBConnection.class);

				try (Statement s = outerDuckDBC.createStatement()) {
					s.execute("CREATE TABLE someTableName (color2 VARCHAR, value2 FLOAT);");
				}

				try (Statement s = innerDuckdbC.createStatement()) {
					s.execute("CREATE TABLE someTableName2 (color2 VARCHAR, value2 FLOAT);");
				}

				Assertions.assertThat(outerDuckDBC).isSameAs(innerDuckdbC);
			}
		}
	}

	@Test
	public void testUnwrap() throws SQLException {
		Assertions.assertThat(dataSource.unwrap(DataSource.class)).isSameAs(dataSource);
		Assertions.assertThat(dataSource.unwrap(DuckDBDataSource.class)).isSameAs(dataSource);
	}

	@Test
	public void testClose() throws SQLException {
		dataSource.close();

		Assertions.assertThatThrownBy(() -> {
			try (Connection c = dataSource.getConnection()) {
				DuckDBConnection duckdbC = ((Wrapper) c).unwrap(DuckDBConnection.class);

				try (Statement s = duckdbC.createStatement()) {
					s.execute("CREATE TABLE someTableName (color VARCHAR, value FLOAT);");
				}
			}
		}).isInstanceOf(IllegalStateException.class).hasRootCauseInstanceOf(SQLException.class);
	}

	@Test
	public void testNotImplemented() throws SQLException {
		Assertions.assertThatThrownBy(() -> dataSource.getLogWriter())
				.isInstanceOf(SQLFeatureNotSupportedException.class);
		Assertions.assertThatThrownBy(() -> dataSource.setLogWriter(new PrintWriter(Mockito.mock(Writer.class))))
				.isInstanceOf(SQLFeatureNotSupportedException.class);
		Assertions.assertThatThrownBy(() -> dataSource.setLoginTimeout(1))
				.isInstanceOf(SQLFeatureNotSupportedException.class);
		Assertions.assertThatThrownBy(() -> dataSource.getLoginTimeout())
				.isInstanceOf(SQLFeatureNotSupportedException.class);
	}
}
