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
package eu.solven.adhoc.table.sql;

import org.jooq.Name;
import org.jooq.TableLike;
import org.jooq.impl.DSL;

import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * Helps configuring a {@link JooqTableWrapper}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class JooqTableWrapperParameters {
	// Default JDBC fetchSize is typically 10
	private static final int DEFAULT_FETCH_SIZE = 16 * 1024;

	@NonNull
	@Default
	IOperatorFactory operatorFactory = StandardOperatorFactory.builder().build();

	@NonNull
	IDSLSupplier dslSupplier;

	@NonNull
	final TableLike<?> table;

	// https://docs.aws.amazon.com/redshift/latest/dg/set-the-JDBC-fetch-size-parameter.html
	// https://stackoverflow.com/questions/1318354/what-does-statement-setfetchsizensize-method-really-do-in-sql-server-jdbc-driv
	// BEWARE We may have multiple concurrent SQL queries, and we may prefer to adjust this given number/type of fetched
	// columns
	// https://github.com/apache/metamodel/blob/master/jdbc/src/main/java/org/apache/metamodel/jdbc/FetchSizeCalculator.java
	// If we encounter OutOfMemoryError, we should lower this parameter.
	final int statementFetchSize = DEFAULT_FETCH_SIZE;

	/**
	 * Lombok @Builder
	 * 
	 * @author Benoit Lacelle
	 */
	public static class JooqTableWrapperParametersBuilder {
		public JooqTableWrapperParametersBuilder tableName(String tableName) {
			this.tableName(DSL.quotedName(tableName));

			return this;
		}

		public JooqTableWrapperParametersBuilder tableName(Name tableName) {
			this.table(DSL.table(tableName));

			return this;
		}
	}
}
