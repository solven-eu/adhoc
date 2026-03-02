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
package eu.solven.adhoc.table.sql.postgresql;

import java.util.UUID;
import java.util.function.Supplier;

import org.jooq.SQLDialect;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.base.Suppliers;
import org.testcontainers.utility.DockerImageName;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.ATestTableQuery_DB;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.TestcontainersSqlHelper;

@Disabled("To be completed")
@Testcontainers(disabledWithoutDocker = true)
public class TestPostgreSqlTableWrapper extends ATestTableQuery_DB implements IAdhocTestConstants {
	@Container
	static final PostgreSQLContainer<?> POSTGRES =
			new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

	// TODO CLose the DataSource properly
	static Supplier<IDSLSupplier> dslSupplierSupplier =
			Suppliers.memoize(() -> TestcontainersSqlHelper.dslSupplier(POSTGRES, SQLDialect.POSTGRES));

	@Override
	protected String tableName() {
		return "test_postgres_%s".formatted(UUID.randomUUID().toString().replace("-", ""));
	}

	@Override
	public IDSLSupplier makeDSLSupplier() {
		return dslSupplierSupplier.get();
	}

	@Override
	public ITableWrapper makeTable() {
		return PostgreSqlTableWrapper.postgresql()
				.name(tableName)
				.postgreSqlParameters(
						PostgreSqlTableWrapperParameters.builder().base(baseJooqTableWrapperParameters()).build())
				.build();
	}

	@Override
	protected @NonNull Class<? extends Throwable> expectedExceptionClassForMissing() {
		return IllegalArgumentException.class;
	}
}
