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

import java.time.Duration;
import java.util.concurrent.Semaphore;

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
@SuppressWarnings("PMD.FieldDeclarationsShouldBeAtStartOfClass")
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
		// Replace the semaphore so its permit count matches the reset value
		querySemaphore = new Semaphore(duckDBParallelism, true);
		semaphoreTimeout = DEFAULT_SEMAPHORE_TIMEOUT;
	}

	public static void reloadProperties() {
		duckDBParallelism = AdhocUnsafe.safeLoadIntegerProperty("adhoc.duckdb.parallelism", DEFAULT_DUCKDB_PARALLELISM);
	}

	/**
	 * Limits the number of concurrent DuckDB queries. DuckDB is highly parallel internally, so submitting many queries
	 * concurrently is counter-productive; a semaphore is more appropriate than a bounded thread pool with Virtual
	 * Threads.
	 *
	 * @return the maximum number of concurrent DuckDB queries
	 */
	@Getter
	private static int duckDBParallelism;

	/**
	 * Sets the maximum number of concurrent DuckDB queries. If the value changes, a new {@link Semaphore} is created
	 * with the updated permit count so that in-flight queries are not affected by a stale semaphore.
	 *
	 * @param parallelism
	 *            the new maximum number of concurrent DuckDB queries; must be positive
	 */
	public static void setDuckDBParallelism(int parallelism) {
		if (duckDBParallelism != parallelism) {
			log.info("Changing duckDBParallelism from {} to {}", duckDBParallelism, parallelism);
			duckDBParallelism = parallelism;
			querySemaphore = new Semaphore(duckDBParallelism);
		}
	}

	private static final int DEFAULT_DUCKDB_PARALLELISM = 2;

	// Semaphore limiting concurrent DuckDB queries.
	// Replaces the former fixed-size thread pool: with Virtual Threads, blocking on I/O is acceptable,
	// but we still want to avoid saturating DuckDB with too many simultaneous queries.
	// It is reasonable to use a static semaphore since all ITableWrapper instances share the same DuckDB process.
	@Getter
	private static Semaphore querySemaphore = new Semaphore(DEFAULT_DUCKDB_PARALLELISM, true);

	private static final Duration DEFAULT_SEMAPHORE_TIMEOUT = Duration.ofMinutes(15);

	@Getter
	private static Duration semaphoreTimeout = DEFAULT_SEMAPHORE_TIMEOUT;

}
