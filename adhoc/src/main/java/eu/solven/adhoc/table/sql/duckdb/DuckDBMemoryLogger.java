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
package eu.solven.adhoc.table.sql.duckdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.duckdb.DuckDBConnection;

import lombok.extern.slf4j.Slf4j;

/**
 * Periodically samples {@code duckdb_memory()} on a DuckDB instance and logs the results via SLF4J.
 *
 * <p>
 * A dedicated {@link DuckDBConnection} is duplicated from the supplied source connection at construction time
 * (following the same pattern as {@link DuckDBDataSource}) and is exclusively used by the internal daemon thread, so it
 * never contends with query-executing threads.
 *
 * <p>
 * Typical usage:
 * 
 * <pre>{@code
 * DuckDBConnection conn = DuckDBHelper.makeFreshInMemoryDb();
 * try (DuckDBMemoryLogger logger = new DuckDBMemoryLogger(conn, Duration.ofSeconds(5))) {
 * 	// ... run queries ...
 * }
 * }</pre>
 *
 * @author Benoit Lacelle
 * @see DuckDBHelper#makeFreshInMemoryDb()
 * @see <a href="https://duckdb.org/docs/stable/sql/meta/duckdb_table_functions.html#duckdb_memory">duckdb_memory()</a>
 */
@Slf4j
public class DuckDBMemoryLogger implements AutoCloseable {

	// https://duckdb.org/docs/stable/sql/meta/duckdb_table_functions.html#duckdb_memory
	private static final String SQL = "SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()";

	protected final DuckDBConnection monitorConn;
	protected final ScheduledExecutorService scheduler;

	/**
	 * Creates and immediately starts the sampling daemon.
	 *
	 * @param source
	 *            the root {@link DuckDBConnection} whose memory is being monitored; a duplicate is created internally
	 *            so the caller's connection is never touched by the daemon thread
	 * @param period
	 *            how often to call {@code duckdb_memory()}
	 */
	public DuckDBMemoryLogger(DuckDBConnection source, Duration period) {
		try {
			monitorConn = source.duplicate();
		} catch (SQLException e) {
			throw new IllegalStateException("Issue duplicating DuckDB connection for memory logging", e);
		}

		scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "duckdb-memory-logger");
			t.setDaemon(true);
			return t;
		});
		scheduler.scheduleAtFixedRate(this::sample, 0, period.toMillis(), TimeUnit.MILLISECONDS);
	}

	protected void sample() {
		try (Statement st = monitorConn.createStatement(); ResultSet rs = st.executeQuery(SQL)) {
			while (rs.next()) {
				String tag = rs.getString("tag");
				long memUsageBytes = rs.getLong("memory_usage_bytes");
				long tmpStorageBytes = rs.getLong("temporary_storage_bytes");
				log.info("DuckDB memory tag={} memory_usage_bytes={} temporary_storage_bytes={}",
						tag,
						memUsageBytes,
						tmpStorageBytes);
			}
		} catch (Exception e) {
			if (isConnectionClosed()) {
				log.info("DuckDB monitoring connection closed — stopping memory logger");
				scheduler.shutdown();
			} else {
				log.warn("Failed to sample duckdb_memory()", e);
			}
		}
	}

	protected boolean isConnectionClosed() {
		try {
			return monitorConn.isClosed();
		} catch (SQLException e) {
			return true;
		}
	}

	@Override
	public void close() {
		scheduler.shutdown();
		try {
			monitorConn.close();
		} catch (SQLException e) {
			log.warn("Failed to close DuckDB monitoring connection", e);
		}
	}
}
