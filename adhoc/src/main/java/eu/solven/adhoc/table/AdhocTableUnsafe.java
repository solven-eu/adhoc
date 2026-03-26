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
package eu.solven.adhoc.table;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Some unsafe operations/configuration related to {@link ITableWrapper}.
 *
 * @author Benoit Lacelle
 */
@Slf4j
@UtilityClass
@Deprecated(since = "Unclear API")
@SuppressWarnings({ "PMD.MutableStaticState", "PMD.FieldDeclarationsShouldBeAtStartOfClass" })
public class AdhocTableUnsafe {

	static {
		reloadProperties();
	}

	public static void resetAll() {
		resetProperties();
	}

	public static void resetProperties() {
		log.info("Resetting {} configuration", AdhocTableUnsafe.class.getName());

		dbParallelism = DEFAULT_DB_PARALLELISM;
	}

	public static void reloadProperties() {
		dbParallelism = AdhocUnsafe.safeLoadIntegerProperty("adhoc.dbParallelism", DEFAULT_DB_PARALLELISM);
	}

	/**
	 * Typically, DuckDB should have very limited concurrency, as it is itself very well parallelized.
	 *
	 * @return the default parallelism of {@link java.util.concurrent.ExecutorService} executing ITableWrapper
	 *         operations.
	 */
	@Getter
	private static int dbParallelism;

	@Deprecated(since = "VERY WRONG as this should be defined on a per-table basis")
	private static final int DEFAULT_DB_PARALLELISM = 2;

	private static final Duration KEEP_ALIVE = Duration.ofMinutes(1);

	// A pool dedicated to external database queries (e.g. DuckDB).
	// Unbounded queue: tasks wait for a DB thread rather than running on the caller's CPU-bound ForkJoinPool thread.
	// Timeout enforcement belongs at the caller side (e.g. future.get(timeout, unit)).
	@Deprecated(since = "VERY WRONG as this should be defined on a per-table basis")
	public static ListeningExecutorService adhocDbPool = MoreExecutors.listeningDecorator(newDbPool("adhoc-db-"));

	/**
	 * Creates a fixed-size thread pool for external database queries with an unbounded queue. Tasks that exceed the
	 * pool capacity wait in the queue rather than running on the caller's CPU-bound thread.
	 *
	 * @return a new {@link ThreadPoolExecutor} sized for DB query concurrency
	 */
	public static ThreadPoolExecutor newDbPool(String prefix) {
		return new ThreadPoolExecutor(dbParallelism,
				dbParallelism,
				KEEP_ALIVE.toSeconds(),
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(),
				Thread.ofPlatform().daemon(true).name(prefix, 0).factory());
	}
}
