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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

public class TestDuckDBMemoryLogger {

	@Test
	public void typicalUsage() throws Exception {
		try (DuckDBConnection conn = DuckDBHelper.makeFreshInMemoryDb();
				DuckDBMemoryLogger logger = new DuckDBMemoryLogger(conn, Duration.ofSeconds(5))) {
			// logger is running; no exception expected
		}
	}

	@Test
	public void sampleIsCalledPeriodically() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);

		try (DuckDBConnection conn = DuckDBHelper.makeFreshInMemoryDb()) {
			DuckDBMemoryLogger logger = new DuckDBMemoryLogger(conn, Duration.ofMillis(50)) {
				@Override
				protected void sample() {
					super.sample();
					latch.countDown();
				}
			};

			try (logger) {
				boolean called = latch.await(2, TimeUnit.SECONDS);
				Assertions.assertThat(called).as("sample() should have been called within 2s").isTrue();
			}
		}
	}

	@Test
	public void closeStopsTheDaemon() throws Exception {
		try (DuckDBConnection conn = DuckDBHelper.makeFreshInMemoryDb()) {
			DuckDBMemoryLogger logger = new DuckDBMemoryLogger(conn, Duration.ofSeconds(60));
			logger.close();

			Assertions.assertThat(logger.scheduler.isShutdown()).isTrue();
		}
	}

	@Test
	public void closedConnectionStopsTheDaemon() throws Exception {
		try (DuckDBConnection conn = DuckDBHelper.makeFreshInMemoryDb()) {

			// Short period so the closed-connection is detected quickly
			try (DuckDBMemoryLogger logger = new DuckDBMemoryLogger(conn, Duration.ofMillis(50))) {

				// Close the root connection to simulate a failure (e.g. exception during query execution)
				conn.close();

				// The next sample() should detect the closed connection and shut down the scheduler
				boolean terminated = logger.scheduler.awaitTermination(2, TimeUnit.SECONDS);
				Assertions.assertThat(terminated)
						.as("scheduler should have self-terminated after connection was closed")
						.isTrue();

			}
		}
	}
}
