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
package eu.solven.adhoc.table.sql.join;

import org.assertj.core.api.Assertions;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.table.sql.AdhocJooqHelper;
import eu.solven.adhoc.table.sql.MissingFilesPolicy;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

/**
 * The DB-probe resolver hits DuckDB with a {@code SELECT * LIMIT 0}: when the base table is a glob of Parquet files
 * matching nothing, DuckDB fails. {@link PrunedJoinsJooqTableSupplier#getMissingFilesPolicy()} decides whether that
 * failure propagates or degrades into "no resolved column".
 */
public class TestPrunedJoinsJooqTableSupplier_MissingFiles {
	static {
		AdhocJooqHelper.disableBanners();
	}

	private static PrunedJoinsJooqTableSupplierBuilder schemaOverMissingFiles() {
		return PrunedJoinsJooqTableSupplierBuilder.prunedBuilder()
				.dslSupplier(DuckDBHelper.inMemoryDSLSupplier())
				.baseTable(DSL.table(DSL.unquotedName("read_parquet('/nonexistent_folder/*.parquet')")))
				.baseTableAlias("fact")
				.build();
	}

	@Test
	public void testDefaultPolicy_noColumn() {
		PrunedJoinsJooqTableSupplier supplier =
				PrunedJoinsJooqTableSupplier.builder().schema(schemaOverMissingFiles()).build();

		Assertions.assertThat(supplier.getMissingFilesPolicy()).isEqualTo(MissingFilesPolicy.WARN);
		Assertions.assertThat(supplier.resolveBaseColumns()).isEmpty();
	}

	@Test
	public void testSilent_noColumn() {
		PrunedJoinsJooqTableSupplier supplier = PrunedJoinsJooqTableSupplier.builder()
				.schema(schemaOverMissingFiles())
				.missingFilesPolicy(MissingFilesPolicy.SILENT)
				.build();

		Assertions.assertThat(supplier.resolveBaseColumns()).isEmpty();
	}

	@Test
	public void testThrow_propagates() {
		PrunedJoinsJooqTableSupplier supplier = PrunedJoinsJooqTableSupplier.builder()
				.schema(schemaOverMissingFiles())
				.missingFilesPolicy(MissingFilesPolicy.THROW)
				.build();

		Assertions.assertThatThrownBy(supplier::resolveBaseColumns)
				.isInstanceOf(DataAccessException.class)
				.hasMessageContaining("No files found that match the pattern");
	}
}
