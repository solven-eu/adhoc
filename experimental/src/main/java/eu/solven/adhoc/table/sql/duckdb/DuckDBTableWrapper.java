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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.duckdb.DuckDBResultSet;
import org.jooq.ConnectionProvider;

import eu.solven.adhoc.table.arrow.AArrowJooqTableWrapper;
import eu.solven.adhoc.table.arrow.ArrowReflection;
import lombok.Builder;

/**
 * Enables querying DuckDB and fetching the result through Arrow.
 *
 * @author Benoit Lacelle
 */
public class DuckDBTableWrapper extends AArrowJooqTableWrapper {

	final DuckDBTableWrapperParameters duckDBParameters;

	@Builder(builderMethodName = "duckdb")
	public DuckDBTableWrapper(String name, DuckDBTableWrapperParameters duckDBParameters) {
		super(name, duckDBParameters.getBase(), duckDBParameters.getMinSplitRows());

		this.duckDBParameters = duckDBParameters;
	}

	@SuppressWarnings("PMD.CloseResource")
	@Override
	protected Object openArrowReader(String sql, List<AutoCloseable> resources) throws SQLException {
		ConnectionProvider connectionProvider = makeDsl().configuration().connectionProvider();
		Connection connection = connectionProvider.acquire();
		resources.add(() -> connectionProvider.release(connection));

		PreparedStatement stmt = connection.prepareStatement(sql);
		resources.add(stmt);

		ResultSet rs = stmt.executeQuery();
		resources.add(rs);

		DuckDBResultSet duckRs = rs.unwrap(DuckDBResultSet.class);

		Object allocator = ArrowReflection.createAllocator();
		resources.add(((AutoCloseable) allocator)::close);

		Object arrowReader = duckRs.arrowExportStream(allocator, duckDBParameters.getArrowBatchSize());
		resources.add(((AutoCloseable) arrowReader)::close);

		return arrowReader;
	}

	@Override
	protected RuntimeException onArrowSqlException(SQLException e) {
		String message = e.getMessage();
		if (message != null && message.contains("Binder Error")) {
			return new IllegalArgumentException("Issue with columns or aggregates in table=" + getName(), e);
		} else if (message != null && message.contains("Catalog Error")) {
			return new IllegalArgumentException("Issue with table in table=" + getName(), e);
		} else {
			return super.onArrowSqlException(e);
		}
	}
}
