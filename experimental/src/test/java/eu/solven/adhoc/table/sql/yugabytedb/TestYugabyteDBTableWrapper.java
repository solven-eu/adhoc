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
package eu.solven.adhoc.table.sql.yugabytedb;

import java.util.Locale;
import java.util.UUID;

import org.jooq.SQLDialect;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.YugabyteDBYSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.ATestTableQuery_DB;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.TestcontainersSqlHelper;

@Disabled("To be completed")
@Testcontainers(disabledWithoutDocker = true)
public class TestYugabyteDBTableWrapper extends ATestTableQuery_DB implements IAdhocTestConstants {
	@Container
	static final YugabyteDBYSQLContainer YUGABYTE =
			new YugabyteDBYSQLContainer(DockerImageName.parse("yugabytedb/yugabyte:2.25.2.0-b359"));

	@Override
	public IDSLSupplier makeDSLSupplier() {
		return TestcontainersSqlHelper.dslSupplier(YUGABYTE, SQLDialect.YUGABYTEDB);
	}

	@Override
	protected String tableName() {
		return (super.tableName() + '_' + UUID.randomUUID().toString().replace('-', '_')).toLowerCase(Locale.US);
	}

	@Override
	public ITableWrapper makeTable() {
		return YugabyteDBTableWrapper.yugabytedb()
				.name(tableName)
				.yugabyteDbParameters(
						YugabyteDbTableWrapperParameters.builder().base(baseJooqTableWrapperParameters()).build())
				.build();
	}

	@Override
	protected @NonNull Class<? extends Throwable> expectedExceptionClassForMissing() {
		return IllegalArgumentException.class;
	}

}
