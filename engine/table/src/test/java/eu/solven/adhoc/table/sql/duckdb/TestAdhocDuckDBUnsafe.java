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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Static-state tests must reset the utility before AND after each test so neighbouring tests in the surefire run see
 * the documented defaults (other DuckDB tests rely on the parallelism semaphore).
 */
public class TestAdhocDuckDBUnsafe {

	@BeforeEach
	@AfterEach
	public void reset() {
		AdhocDuckDBUnsafe.resetAll();
	}

	@Test
	public void testDefaults_areApplied() {
		Assertions.assertThat(AdhocDuckDBUnsafe.getDuckDBParallelism()).isEqualTo(2);
		Assertions.assertThat(AdhocDuckDBUnsafe.getSemaphoreTimeout()).isEqualTo(Duration.ofMinutes(15));
		Assertions.assertThat(AdhocDuckDBUnsafe.getQuerySemaphore().availablePermits()).isEqualTo(2);
	}

	@Test
	public void testSetDuckDBParallelism_changesValue_andReplacesSemaphore() {
		Semaphore initial = AdhocDuckDBUnsafe.getQuerySemaphore();

		AdhocDuckDBUnsafe.setDuckDBParallelism(8);

		Assertions.assertThat(AdhocDuckDBUnsafe.getDuckDBParallelism()).isEqualTo(8);
		// The semaphore is rebuilt with the new permit count when parallelism changes.
		Assertions.assertThat(AdhocDuckDBUnsafe.getQuerySemaphore()).isNotSameAs(initial);
		Assertions.assertThat(AdhocDuckDBUnsafe.getQuerySemaphore().availablePermits()).isEqualTo(8);
	}

	@Test
	public void testSetDuckDBParallelism_sameValue_keepsSemaphore() {
		Semaphore initial = AdhocDuckDBUnsafe.getQuerySemaphore();

		AdhocDuckDBUnsafe.setDuckDBParallelism(AdhocDuckDBUnsafe.getDuckDBParallelism());

		Assertions.assertThat(AdhocDuckDBUnsafe.getQuerySemaphore()).isSameAs(initial);
	}

	@Test
	public void testReset_restoresDefaults() {
		AdhocDuckDBUnsafe.setDuckDBParallelism(16);

		AdhocDuckDBUnsafe.resetProperties();

		Assertions.assertThat(AdhocDuckDBUnsafe.getDuckDBParallelism()).isEqualTo(2);
		Assertions.assertThat(AdhocDuckDBUnsafe.getQuerySemaphore().availablePermits()).isEqualTo(2);
		Assertions.assertThat(AdhocDuckDBUnsafe.getSemaphoreTimeout()).isEqualTo(Duration.ofMinutes(15));
	}

	@Test
	public void testReloadProperties_picksUpSystemProperty() {
		System.setProperty("adhoc.duckdb.parallelism", "5");
		try {
			AdhocDuckDBUnsafe.reloadProperties();
			Assertions.assertThat(AdhocDuckDBUnsafe.getDuckDBParallelism()).isEqualTo(5);
		} finally {
			System.clearProperty("adhoc.duckdb.parallelism");
		}
	}
}
