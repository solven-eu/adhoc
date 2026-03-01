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
package eu.solven.adhoc.table.sql.clickhouse;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.jooq.Record;
import org.jooq.ResultQuery;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;

import eu.solven.adhoc.table.arrow.ArrowJooqTableWrapper;
import eu.solven.adhoc.table.arrow.ArrowReflection;
import lombok.Builder;

/**
 * Enables querying ClickHouse through JDBC and jOOQ while streaming the results via Arrow.
 *
 * @author Benoit Lacelle
 */
public class ClickHouseTableWrapper extends ArrowJooqTableWrapper {

	final ClickHouseTableWrapperParameters clickHouseParameters;

	@Builder(builderMethodName = "clickhouse")
	public ClickHouseTableWrapper(String name, ClickHouseTableWrapperParameters clickHouseParameters) {
		super(name, clickHouseParameters.getBase(), clickHouseParameters.getMinSplitRows());

		this.clickHouseParameters = clickHouseParameters;
	}

	@Override
	protected String getSQL(ResultQuery<Record> sqlQuery) {
		return super.getSQL(sqlQuery) + " FORMAT ArrowStream";
	}

	@Override
	protected Object openArrowReader(String sql, List<AutoCloseable> resources) throws SQLException {
		Client client = clickHouseParameters.getClient();

		CompletableFuture<QueryResponse> request = client.query(sql);

		try (QueryResponse response = request.get()) {
			InputStream arrowStream = response.getInputStream();
			resources.add(arrowStream::close);

			Object allocator = ArrowReflection.createAllocator();
			resources.add(((AutoCloseable) allocator)::close);

			Object arrowReader = ArrowReflection.createStreamReader(arrowStream, allocator);
			resources.add(((AutoCloseable) arrowReader)::close);

			return arrowReader;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
