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
package eu.solven.adhoc.table.sql.mysql;

import org.jooq.SQLDialect;
import org.jspecify.annotations.NonNull;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.ATestTableQuery_DB;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.TestcontainersSqlHelper;

@Testcontainers(disabledWithoutDocker = true)
public class TestMySQLTableWrapper extends ATestTableQuery_DB implements IAdhocTestConstants {
	@Container
	static final MySQLContainer<?> MYSQL = new MySQLContainer<>(DockerImageName.parse("mysql:8.4"));

	@Override
	public IDSLSupplier makeDSLSupplier() {
		return TestcontainersSqlHelper.dslSupplier(MYSQL, SQLDialect.MYSQL);
	}

	@Override
	public ITableWrapper makeTable() {
		return MySqlTableWrapper.mysql()
				.name(tableName)
				.mySqlParameters(MySqlTableWrapperParameters.builder().base(baseJooqTableWrapperParameters()).build())
				.build();
	}

	@Override
	protected @NonNull Class<? extends Throwable> expectedExceptionClassForMissing() {
		return IllegalArgumentException.class;
	}
}
