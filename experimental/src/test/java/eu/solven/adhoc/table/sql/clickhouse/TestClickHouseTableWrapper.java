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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.SQLDataType;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.clickhouse.client.api.ServerException;

import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.ATestTableQuery_DB;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.TestcontainersSqlHelper;

// TODO Check these tests are fine in CI
@Testcontainers(disabledWithoutDocker = true)
public class TestClickHouseTableWrapper extends ATestTableQuery_DB {

	@Container
	static final ClickHouseContainer CLICKHOUSE =
			new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:24.8-alpine"));

	@Override
	protected String tableName() {
		return "test_clickhouse_%s".formatted(UUID.randomUUID().toString().replace("-", ""));
	}

	@Override
	public IDSLSupplier makeDSLSupplier() {
		return TestcontainersSqlHelper.dslSupplier(CLICKHOUSE, SQLDialect.CLICKHOUSE);
	}

	@Override
	public ITableWrapper makeTable() {
		return ClickHouseTableWrapper.clickhouse()
				.name(tableName)
				.clickHouseParameters(
						ClickHouseTableWrapperParameters.builder().base(baseJooqTableWrapperParameters()).build())
				.build();
	}

	@Override
	protected @NonNull Class<? extends Throwable> expectedExceptionClassForMissing() {
		return DataAccessException.class;
	}

	@Override
	protected String expectedMessageForMissingTable() {
		return "Unknown table expression identifier";
	}

	@Override
	protected Class<? extends Throwable> expectedExceptionClassForSumOverVarchar() {
		return DataAccessException.class;
	}

	@Override
	protected String expectedMessageForSumOverVarchar() {
		return "Illegal type String";
	}

	@Override
	protected String expectedMessageForUnknownGroupByColumn() {
		return "Unknown expression ";
	}

	@Override
	protected @Nullable Class<? extends Throwable> expectedRootCauseForUnknownColumn() {
		return ServerException.class;
	}

	@Test
	public void testEmptyDb() {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.DOUBLE).execute();

		List<Map<String, ?>> tableStream = table().streamSlices(qK1).toList();

		// ClickHouse returns an empty result in such a case
		Assertions.assertThat(tableStream).isEmpty();
	}

	@Test
	public void testCubeQuery_sumFilterGroupByk1() {
		Assertions.assertThatThrownBy(() -> super.testCubeQuery_sumFilterGroupByk1())
				.hasRootCauseExactlyInstanceOf(ServerException.class)
				.hasStackTraceContaining(
						"Code: 184. DB::Exception: Aggregate function sum(k1) AS k1 is found in GROUP BY in query. (ILLEGAL_AGGREGATION)");
	}

	// This is an actual bug: if we end requesting twice the same aggregate, one with matchAll and other with a leftover
	// filter, we should query the aggregate only once to DB
	@Disabled("TODO FIXME")
	@Test
	public void testFilter_custom_implicitColumns() {
		super.testFilter_custom_implicitColumns();
	}

	@Disabled("TODO May be a limitation in ClickHouse")
	@Test
	public void testFilterOnAggregates() {
		super.testFilterOnAggregates();
	}

	@Disabled("TODO May be a limitation in ClickHouse")
	@Test
	public void testFilterOnAggregates_aggregateNameIsAlsoColumnName() {
		super.testFilterOnAggregates_aggregateNameIsAlsoColumnName();
	}

	@Disabled("TODO https://github.com/jOOQ/jOOQ/issues/19695")
	@Test
	public void testFilterOnLocalDate_asString() {
		super.testFilterOnLocalDate_asString();
	}
}
