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
package eu.solven.adhoc.table.sql.duckdb;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.table.AdhocTableUnsafe;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Some unsafe/advanced operations/configurations related with DuckDB
 *
 * @author Benoit Lacelle
 */
@Slf4j
@UtilityClass
@SuppressWarnings({ "PMD.MutableStaticState", "PMD.FieldDeclarationsShouldBeAtStartOfClass" })
public class AdhocDuckDBUnsafe {

	static {
		reloadProperties();
	}

	public static void resetAll() {
		resetProperties();
	}

	public static void resetProperties() {
		log.info("Resetting {} configuration", AdhocDuckDBUnsafe.class.getName());

		duckDBParallelism = DEFAULT_DUCKDB_PARALLELISM;
	}

	public static void reloadProperties() {
		duckDBParallelism = AdhocUnsafe.safeLoadIntegerProperty("adhoc.duckdb.parallelism", DEFAULT_DUCKDB_PARALLELISM);
	}

	/**
	 * Typically, DuckDB should have very limited concurrency, as it is itself very well parallelized.
	 *
	 * @return the default parallelism of {@link java.util.concurrent.ExecutorService} executing ITableWrapper
	 *         operations.
	 */
	@Getter
	private static int duckDBParallelism;

	private static final int DEFAULT_DUCKDB_PARALLELISM = 2;

	// A pool dedicated to external database queries (e.g. DuckDB).
	// Unbounded queue: tasks wait for a DB thread rather than running on the caller's CPU-bound ForkJoinPool thread.
	// Timeout enforcement belongs at the caller side (e.g. future.get(timeout, unit)).
	// It is reasonable to have a static pool as even if we use multiple ITableWrapper, they would all rely on the same
	// resource (i.e. current OS).
	public static ListeningExecutorService adhocDuckDBPool =
			MoreExecutors.listeningDecorator(AdhocTableUnsafe.newDbPool("adhoc-duckdbdb-"));

}
