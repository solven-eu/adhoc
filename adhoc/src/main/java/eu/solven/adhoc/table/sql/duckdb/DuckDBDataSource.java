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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Wrapper;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.duckdb.DuckDBConnection;
import org.jooq.tools.jdbc.DefaultConnection;
import org.jooq.tools.jdbc.SingleConnectionDataSource;

import lombok.extern.slf4j.Slf4j;

/**
 * A {@link DataSource} that wraps a {@link DuckDBConnection} connection, preventing its closing when it is obtained
 * from this data source.
 *
 * @author Benoit Lacelle
 * @see SingleConnectionDataSource
 */
@Slf4j
public class DuckDBDataSource implements DataSource, AutoCloseable {

	DuckDBConnection delegate;
	ThreadLocal<DuckDBConnection> threadLocal;
	ThreadLocal<AtomicInteger> threadToNbOpen = ThreadLocal.withInitial(() -> new AtomicInteger());

	public DuckDBDataSource(DuckDBConnection delegate) {
		this.delegate = delegate;
		threadLocal = ThreadLocal.withInitial(() -> {
			DuckDBConnection duplicated;
			try {
				duplicated = delegate.duplicate();
			} catch (SQLException e) {
				throw new IllegalStateException("Issue duplicating an InMemory DuckDB connection", e);
			}
			return duplicated;
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (isWrapperFor(iface)) {
			return (T) this;
		} else {
			throw new SQLException("DataSource does not implement " + iface);
		}
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	/**
	 * Wrapper for {@link DuckDBConnection}
	 */
	public static class DuckDBConnectionWrapper extends DefaultConnection implements Wrapper {
		protected DuckDBConnection delegate;
		protected AtomicInteger openCounter;
		protected Runnable onClose;

		public DuckDBConnectionWrapper(DuckDBConnection delegate, AtomicInteger openCounter, Runnable onClose) {
			super(delegate);
			this.delegate = delegate;
			this.openCounter = openCounter;
			this.onClose = onClose;
		}

		@Override
		public void close() throws SQLException {
			int nbOpen = openCounter.decrementAndGet();
			if (nbOpen == 0) {
				log.debug("Thread={} is being closed", Thread.currentThread().getName());
				delegate.close();
				onClose.run();
			} else {
				log.debug("Thread={} is still open {} times", Thread.currentThread().getName(), nbOpen);
			}
		}
	}

	@Override
	public Connection getConnection() throws SQLException {
		AtomicInteger openCounter = threadToNbOpen.get();
		openCounter.incrementAndGet();

		DuckDBConnection threadConnection = threadLocal.get();
		return new DuckDBConnectionWrapper(threadConnection, openCounter, threadLocal::remove);
	}

	@Override
	public void close() throws SQLException {
		delegate.close();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// Cannot use Logger.getGlobal() in JDK 6 yet
		return Logger.getAnonymousLogger().getParent();
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		throw new SQLFeatureNotSupportedException("SingleConnectionDataSource cannot create new connections");
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		throw new SQLFeatureNotSupportedException("getLogWriter");
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		throw new SQLFeatureNotSupportedException("setLogWriter");
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		throw new SQLFeatureNotSupportedException("setLoginTimeout");
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException("getLoginTimeout");
	}
}
