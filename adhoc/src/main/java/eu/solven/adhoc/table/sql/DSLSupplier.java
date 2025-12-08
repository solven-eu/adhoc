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
package eu.solven.adhoc.table.sql;

import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;

/**
 * Helps building a proper {@link DSLContext}, typically for {@link JooqTableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
@FunctionalInterface
public interface DSLSupplier {
	DSLContext getDSLContext();

	/**
	 * Without a connection or data source, this executor cannot execute queries. Use it to render SQL only.
	 * 
	 * @param sqlDialect
	 * @return a {@link DSLSupplier} based on provided {@link SQLDialect}
	 */
	static DSLSupplier fromDialect(SQLDialect sqlDialect) {
		return StandardDSLSupplier.builder().dialect(sqlDialect).build();
	}

	static DSLSupplier fromDatasource(DataSource datasource, SQLDialect sqlDialect) {
		return StandardDSLSupplier.builder().dialect(sqlDialect).dataSource(datasource).build();
	}

	@Deprecated(since = "Unstable API")
	static DSLSupplier fromDatasourceStopWatch(DataSource datasource, SQLDialect sqlDialect) {
		return StandardDSLSupplier.builder().dialect(sqlDialect).dataSource(datasource).stopWatch().build();
	}
}
