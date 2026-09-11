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

import java.util.Objects;

import org.jspecify.annotations.Nullable;

import lombok.extern.slf4j.Slf4j;

/**
 * How a {@link JooqTableWrapper} reacts when the SQL engine reports that a file-backed table matches not a single
 * file. The typical case is DuckDB reading a glob of Parquet files (e.g. {@code read_parquet('folder/*.parquet')})
 * while the folder is empty or absent: DuckDB fails with {@code IO Error: No files found that match the pattern}.
 * <p>
 * Tolerant policies turn that failure into "the table is empty": column discovery reports no column, queries stream no
 * row, and join-pruning probes resolve no column.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public enum MissingFilesPolicy {
	/** Behave as an empty table, and log a WARN. This is the default. */
	WARN,
	/**
	 * Behave as an empty table, logging only at DEBUG level. Useful when many queries legitimately hit tables whose
	 * files are absent, to prevent flooding the logs.
	 */
	SILENT,
	/** Propagate the SQL engine's exception, i.e. DuckDB default behavior. */
	THROW;

	// DuckDB message. The `IO Error:` prefix is not matched as JDBC drivers may wrap the message differently.
	static final String DUCKDB_NO_FILES = "No files found that match the pattern";

	// Bounds the walk through the causes, in case of a cyclic chain
	private static final int MAX_CAUSE_DEPTH = 16;

	/**
	 * @param t
	 *            a failure, typically a {@link org.jooq.exception.DataAccessException}
	 * @return true if {@code t}, or one of its causes, is the SQL engine reporting that a file-backed table matches no
	 *         file.
	 */
	public static boolean isMissingFilesError(@Nullable Throwable t) {
		Throwable current = t;
		int depth = 0;
		while (current != null && depth < MAX_CAUSE_DEPTH) {
			if (Objects.requireNonNullElse(current.getMessage(), "").contains(DUCKDB_NO_FILES)) {
				return true;
			}
			current = current.getCause();
			depth++;
		}
		return false;
	}

	/**
	 * Reacts to a missing-files failure: returns normally when the policy tolerates it (the caller then behaves as if
	 * the table were empty), or rethrows {@code e} under {@link #THROW}.
	 *
	 * @param e
	 *            the failure, expected to satisfy {@link #isMissingFilesError(Throwable)}
	 * @param context
	 *            describes the failing operation (table, query) for logs
	 */
	public void onMissingFiles(RuntimeException e, Object context) {
		if (this == THROW) {
			throw e;
		} else if (this == SILENT) {
			log.debug("Behaving as an empty table due to missing files. context={}", context, e);
		} else if (log.isDebugEnabled()) {
			log.warn("Behaving as an empty table due to missing files. context={}", context, e);
		} else {
			// The failure may be anywhere in the SQL (e.g. the main `FROM`, or any `JOIN`): the message tells which
			log.warn("Behaving as an empty table due to missing files. context={} sqlMsg={}", context, e.getMessage());
		}
	}
}
