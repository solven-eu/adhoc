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
package eu.solven.adhoc.table.sql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocTestHelper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.measure.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;

public class TestJooqTableWrapper implements IAdhocTestConstants {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	CubeQueryEngine engine = CubeQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

	@Test
	public void testTableIsFunctionCall() throws IOException, SQLException {
		// Duplicated from TestDatabaseQuery_DuckDb_FromParquet
		Path tmpParquetPath = Files.createTempFile(this.getClass().getSimpleName(), ".parquet");
		String tableName = "%s".formatted(tmpParquetPath.toAbsolutePath());

		String tableExpression = "read_parquet('%s', union_by_name=True)".formatted(tableName);

		try {
			DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();
			JooqTableWrapperParameters dbParameters = JooqTableWrapperParameters.builder()
					.dslSupplier(dslSupplier)
					.tableName(DSL.unquotedName(tableExpression))
					.build();
			JooqTableWrapper jooqDb = new JooqTableWrapper("fromParquet", dbParameters);

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
				UnsafeMeasureForest forest = UnsafeMeasureForest.builder().name("jooq").build();
				forest.addMeasure(k1Sum);
				CubeWrapper aqw = CubeWrapper.builder().table(jooqDb).engine(engine).forest(forest).build();

				ITabularView result = aqw.execute(CubeQuery.builder().measure(k1Sum.getName()).build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(result);

				Assertions.assertThat(mapBased.getCoordinatesToValues())
						.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 234));
			}
		} finally {
			Files.delete(tmpParquetPath);
		}
	}

	@Test
	public void testCancel() throws IOException, SQLException {
		// Duplicated from TestDatabaseQuery_DuckDb_FromParquet
		Path tmpParquetPath = Files.createTempFile(this.getClass().getSimpleName(), ".parquet");
		String tableName = "%s".formatted(tmpParquetPath.toAbsolutePath());

		String tableExpression = "read_parquet('%s', union_by_name=True)".formatted(tableName);

		try {
			DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();
			JooqTableWrapperParameters dbParameters = JooqTableWrapperParameters.builder()
					.dslSupplier(dslSupplier)
					.tableName(DSL.unquotedName(tableExpression))
					.build();
			JooqTableWrapper jooqDb = new JooqTableWrapper("fromParquet", dbParameters);

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
				QueryPod queryPod = QueryPod.forTable(jooqDb);
				ITabularRecordStream stream = jooqDb.streamSlices(queryPod,
						TableQueryV2.builder()
								.aggregator(FilteredAggregator.builder().aggregator(Aggregator.sum("k1")).build())
								.build());

				queryPod.cancel();

				List<Map<String, ?>> asList = stream.toList();

				// BEWARE We seemingly receive a result as the query is so small it is fully executed when cancelled
				Assertions.assertThat(asList).hasSize(1).contains(Map.of("k1", 0L + 357));
			}
		} finally {
			Files.delete(tmpParquetPath);
		}
	}

	@Test
	public void testGetDetails() throws IOException, SQLException {
		// Duplicated from TestDatabaseQuery_DuckDb_FromParquet
		Path tmpParquetPath = Files.createTempFile(this.getClass().getSimpleName(), ".parquet");
		String tableName = "%s".formatted(tmpParquetPath.toAbsolutePath());

		String tableExpression = "read_parquet('%s', union_by_name=True)".formatted(tableName);

		try {
			DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();
			JooqTableWrapperParameters dbParameters = JooqTableWrapperParameters.builder()
					.dslSupplier(dslSupplier)
					.tableName(DSL.unquotedName(tableExpression))
					.build();
			JooqTableWrapper jooqDb = new JooqTableWrapper("fromParquet", dbParameters);

			Assertions.assertThat((Map) jooqDb.getHealthDetails())
					.containsEntry("dialect", SQLDialect.DUCKDB)
					.containsKey("dslContextCreationTime")
					.containsEntry("tableLike", tableExpression)
					.hasSize(3);
		} finally {
			Files.delete(tmpParquetPath);
		}
	}
}
