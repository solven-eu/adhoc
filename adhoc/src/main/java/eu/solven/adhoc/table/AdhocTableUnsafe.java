package eu.solven.adhoc.table;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Benoit Lacelle
 */
@Slf4j
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
     * @return the default parallelism of {@link java.util.concurrent.ExecutorService} executing ITableWrapper operations.
     */
    @Getter
    private static int dbParallelism;

    private static final int DEFAULT_DB_PARALLELISM = 2;

    // A pool dedicated to external database queries (e.g. DuckDB).
    // Unbounded queue: tasks wait for a DB thread rather than running on the caller's CPU-bound ForkJoinPool thread.
    // Timeout enforcement belongs at the caller side (e.g. future.get(timeout, unit)).
    public static ListeningExecutorService adhocDbPool = MoreExecutors.listeningDecorator(newDbPool());

    /**
     * Creates a fixed-size thread pool for external database queries with an unbounded queue. Tasks that exceed the pool
     * capacity wait in the queue rather than running on the caller's CPU-bound thread.
     *
     * @return a new {@link ThreadPoolExecutor} sized for DB query concurrency
     */
    protected static ThreadPoolExecutor newDbPool() {
        return new ThreadPoolExecutor(dbParallelism,
                dbParallelism,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                Thread.ofPlatform().daemon(true).name("adhoc-db-", 0).factory());
    }
}
