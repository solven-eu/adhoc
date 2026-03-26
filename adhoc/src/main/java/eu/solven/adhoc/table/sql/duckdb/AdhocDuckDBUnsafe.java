package eu.solven.adhoc.table.sql.duckdb;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Some unsafe/advanced operations/configurations related with DuckDB
 *
 * @author Benoit Lacelle
 */
@Slf4j
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
     * @return the default parallelism of {@link java.util.concurrent.ExecutorService} executing ITableWrapper operations.
     */
    @Getter
    private static int duckDBParallelism;

    private static final int DEFAULT_DUCKDB_PARALLELISM = 2;

    // A pool dedicated to external database queries (e.g. DuckDB).
    // Unbounded queue: tasks wait for a DB thread rather than running on the caller's CPU-bound ForkJoinPool thread.
    // Timeout enforcement belongs at the caller side (e.g. future.get(timeout, unit)).
    public static ListeningExecutorService adhocDuckDBPool = MoreExecutors.listeningDecorator(newDbPool());

    /**
     * Creates a fixed-size thread pool for external database queries with an unbounded queue. Tasks that exceed the pool
     * capacity wait in the queue rather than running on the caller's CPU-bound thread.
     *
     * @return a new {@link ThreadPoolExecutor} sized for DB query concurrency
     */
    protected static ThreadPoolExecutor newDbPool() {
        return new ThreadPoolExecutor(duckDBParallelism,
                duckDBParallelism,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                Thread.ofPlatform().daemon(true).name("adhoc-db-", 0).factory());
    }
}
