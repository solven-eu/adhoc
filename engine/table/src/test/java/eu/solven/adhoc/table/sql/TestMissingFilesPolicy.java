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
package eu.solven.adhoc.table.sql;

import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

public class TestMissingFilesPolicy {
	static final String DUCKDB_MESSAGE = "IO Error: No files found that match the pattern \"/tmp/folder/*.parquet\"";

	@Test
	public void testIsMissingFilesError_null() {
		Assertions.assertThat(MissingFilesPolicy.isMissingFilesError(null)).isFalse();
	}

	@Test
	public void testIsMissingFilesError_unrelated() {
		Assertions.assertThat(MissingFilesPolicy.isMissingFilesError(new DataAccessException("Syntax error"))).isFalse();
		Assertions.assertThat(MissingFilesPolicy.isMissingFilesError(new DataAccessException(null))).isFalse();
	}

	@Test
	public void testIsMissingFilesError_direct() {
		Assertions.assertThat(MissingFilesPolicy.isMissingFilesError(new DataAccessException(DUCKDB_MESSAGE))).isTrue();
	}

	@Test
	public void testIsMissingFilesError_nestedCause() {
		DataAccessException wrapped =
				new DataAccessException("SQL [select * from t limit 0]; failed", new SQLException(DUCKDB_MESSAGE));

		Assertions.assertThat(MissingFilesPolicy.isMissingFilesError(wrapped)).isTrue();
	}

	@Test
	public void testOnMissingFiles_throw() {
		DataAccessException e = new DataAccessException(DUCKDB_MESSAGE);

		Assertions.assertThatThrownBy(() -> MissingFilesPolicy.THROW.onMissingFiles(e, "someTable")).isSameAs(e);
	}

	@Test
	public void testOnMissingFiles_warn() {
		DataAccessException e = new DataAccessException(DUCKDB_MESSAGE);

		Assertions.assertThatCode(() -> MissingFilesPolicy.WARN.onMissingFiles(e, "someTable"))
				.doesNotThrowAnyException();
	}

	@Test
	public void testOnMissingFiles_silent() {
		DataAccessException e = new DataAccessException(DUCKDB_MESSAGE);

		Assertions.assertThatCode(() -> MissingFilesPolicy.SILENT.onMissingFiles(e, "someTable"))
				.doesNotThrowAnyException();
	}

	@Test
	public void testDefaultPolicy_tableParameters() {
		JooqTableWrapperParameters parameters = JooqTableWrapperParameters.builder()
				.dslSupplier(DuckDBHelper.inMemoryDSLSupplier())
				.tableName("someTable")
				.build();

		Assertions.assertThat(parameters.getMissingFilesPolicy()).isEqualTo(MissingFilesPolicy.WARN);
	}
}
