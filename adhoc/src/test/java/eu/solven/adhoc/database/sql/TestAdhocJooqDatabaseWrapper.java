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
package eu.solven.adhoc.database.sql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.slice.AdhocSliceAsMap;

public class TestAdhocJooqDatabaseWrapper implements IAdhocTestConstants {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	@Test
	public void testTableIsFunctionCall() throws IOException, SQLException {
		// Duplicated from TestDatabaseQuery_DuckDb_FromParquet
		Path tmpParquetPath = Files.createTempFile(this.getClass().getSimpleName(), ".parquet");
		String tableName = "%s".formatted(tmpParquetPath.toAbsolutePath());

		String tableExpression = "read_parquet('%s', union_by_name=True)".formatted(tableName);

		try (Connection dbConn = DuckDbHelper.makeFreshInMemoryDb()) {
			AdhocJooqDatabaseWrapperParameters dbParameters = AdhocJooqDatabaseWrapperParameters.builder()
					.dslSupplier(DSLSupplier.fromConnection(() -> dbConn))
					.tableName(DSL.unquotedName(tableExpression))
					.build();
			AdhocJooqDatabaseWrapper jooqDb = new AdhocJooqDatabaseWrapper(dbParameters);

			DSLContext dsl = jooqDb.makeDsl();

			// We create a simple parquet files, as we want to test the `read_parquet` expression as tableName
			{
				dsl.execute("""
						CREATE TABLE someTableName AS
							SELECT 'a1' AS a, 123 AS k1 UNION ALL
							SELECT 'a2' AS a, 234 AS k1
							;
						""");
				dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));
			}

			{
				AdhocMeasureBag measureBag = AdhocMeasureBag.builder().build();
				measureBag.addMeasure(k1Sum);

				AdhocQueryEngine aqe =
						AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()).measureBag(measureBag).build();

				ITabularView result = aqe.execute(AdhocQuery.builder().measure(k1Sum.getName()).build(), jooqDb);
				MapBasedTabularView mapBased = MapBasedTabularView.load(result);

				Assertions.assertThat(mapBased.keySet().map(AdhocSliceAsMap::getCoordinates).toList())
						.containsExactly(Map.of());
				Assertions.assertThat(mapBased.getCoordinatesToValues())
						.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 234));
			}
		} finally {
			Files.delete(tmpParquetPath);
		}
	}
}
