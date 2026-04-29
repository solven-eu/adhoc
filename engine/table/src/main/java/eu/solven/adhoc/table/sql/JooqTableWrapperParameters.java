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

import java.util.concurrent.Semaphore;

import org.jooq.Name;
import org.jooq.Table;
import org.jooq.TableLike;
import org.jooq.impl.DSL;

import eu.solven.adhoc.filter.optimizer.IFilterOptimizerFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqTableSupplierBuilder;
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
	@Default
	IFilterOptimizerFactory filterOptimizerFactory = IFilterOptimizerFactory.standard();

	@NonNull
	IDSLSupplier dslSupplier;

	// @NonNull
	// final TableLike<?> table;

	/**
	 * Optional per-query {@link TableLike} provider. When set, {@link JooqTableWrapper#streamSlices} substitutes the
	 * {@link #table} field with {@link IJooqTableSupplier#tableFor(eu.solven.adhoc.query.table.TableQueryV4)} when
	 * producing the SQL {@code FROM} clause. Schema introspection ({@code getResultForFields}, {@code getColumns})
	 * keeps using the all-joins {@link #table}.
	 * <p>
	 * Prefer wiring this via the builder's {@code tableSupplier(IJooqTableSupplier)} setter (typically with
	 * {@code schema.build()}): it sets both {@link #table} and {@link #tableSupplier} from the supplier's
	 * {@link IJooqTableSupplier#getSchemaTable()}, so callers no longer need a separate {@code .table(...)} call.
	 */
	IJooqTableSupplier tableSupplier;

	/**
	 * Strategy for discovering a {@link TableLike}'s fields. Consumed by {@link JooqTableWrapper#getColumns()} and
	 * shared with {@link PrunedJoinsJooqTableSupplierBuilder} so both customise in lockstep. When not configured by the
	 * builder, {@link #getColumnsResolver()} returns a {@link JooqColumnsHelpers#dbProbe(IDSLSupplier)} bound to
	 * {@link #getDslSupplier()} — i.e. a {@code SELECT * LIMIT 0} probe.
	 */
	@Default
	final IJooqColumnsResolver columnsResolver = JooqColumnsHelpers.dbProbe();

	// https://docs.aws.amazon.com/redshift/latest/dg/set-the-JDBC-fetch-size-parameter.html
	// https://stackoverflow.com/questions/1318354/what-does-statement-setfetchsizensize-method-really-do-in-sql-server-jdbc-driv
	// BEWARE We may have multiple concurrent SQL queries, and we may prefer to adjust this given number/type of fetched
	// columns
	// https://github.com/apache/metamodel/blob/master/jdbc/src/main/java/org/apache/metamodel/jdbc/FetchSizeCalculator.java
	// If we encounter OutOfMemoryError, we should lower this parameter.
	@Default
	final int statementFetchSize = DEFAULT_FETCH_SIZE;

	/**
	 * Optional semaphore limiting concurrent queries against this table.
	 * <p>
	 * Useful for databases that perform best with bounded concurrency (e.g. DuckDB). With Virtual Threads, a semaphore
	 * is the right primitive — a bounded thread pool is no longer needed.
	 */
	@Default
	@NonNull
	Semaphore querySemaphore = new Semaphore(Integer.MAX_VALUE);

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
			return this.table(DSL.table(tableName));
		}

		public JooqTableWrapperParametersBuilder table(Table<?> table) {
			this.tableSupplier(IJooqTableSupplier.constant(table));

			return this;
		}
	}
}
