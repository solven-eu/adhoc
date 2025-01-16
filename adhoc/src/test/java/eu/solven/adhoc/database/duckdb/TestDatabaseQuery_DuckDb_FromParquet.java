/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.database.duckdb;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.database.sql.AdhocJooqSqlDatabaseWrapper;
import eu.solven.adhoc.database.sql.DSLSupplier;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.slice.AdhocSliceAsMap;

public class TestDatabaseQuery_DuckDb_FromParquet implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
	}

	Path tmpParquetPath;
	DatabaseQuery qK1 = DatabaseQuery.builder().aggregators(Set.of(k1Sum)).build();

	DSLContext dsl;
	AdhocJooqSqlDatabaseWrapper jooqDb;

	@BeforeEach
	public void initParquetFiles() throws IOException {
		tmpParquetPath = Files.createTempFile(this.getClass().getSimpleName(), ".parquet");

		String tableName = "%s".formatted(tmpParquetPath.toAbsolutePath());

		Connection dbConn = makeFreshInMemoryDb();
		jooqDb = AdhocJooqSqlDatabaseWrapper.builder()
				.dslSupplier(DSLSupplier.fromConnection(() -> dbConn))
				.tableName(DSL.name(tableName))
				.build();

		dsl = jooqDb.makeDsl();
	}

	@AfterEach
	public void afterEach() throws IOException {
		if (Files.exists(tmpParquetPath)) {
			// Remove temporary files
			Files.delete(tmpParquetPath);
		}
	}

	private Connection makeFreshInMemoryDb() {
		try {
			return DriverManager.getConnection("jdbc:duckdb:");
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	@Test
	public void testFileIsEmpty() {
		Assertions.assertThatThrownBy(() -> jooqDb.openDbStream(qK1).toList()).isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testTableDoesNotExists() throws IOException {
		Files.delete(tmpParquetPath);

		Assertions.assertThatThrownBy(() -> jooqDb.openDbStream(qK1).toList()).isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testEmptyDb() {
		dsl.execute("CREATE OR REPLACE TABLE someTableName (k1 DECIMAL);");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).toList();

		Assertions.assertThat(dbStream).isEmpty();
	}

	@Test
	public void testReturnAll() {
		dsl.execute("CREATE TABLE someTableName AS SELECT 123 AS k1;");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).toList();

		Assertions.assertThat(dbStream).hasSize(1).contains(Map.of("k1", BigDecimal.valueOf(123)));
	}

	@Test
	public void testSumOnVarchar() {
		dsl.execute("CREATE TABLE someTableName AS SELECT 'someKey' AS k1;");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		Assertions.assertThatThrownBy(() -> jooqDb.openDbStream(qK1).toList()).isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testAdhocQuery_GrandTotal() {
		dsl.execute("""
				CREATE TABLE someTableName AS
					SELECT 'a1' AS a, 123 AS k1 UNION ALL
					SELECT 'a2' AS a, 234 AS k1
					;
				""");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		AdhocMeasureBag measureBag = AdhocMeasureBag.builder().build();
		measureBag.addMeasure(k1Sum);
		measureBag.addMeasure(k1SumSquared);

		AdhocQueryEngine aqe =
				AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()).measureBag(measureBag).build();

		ITabularView result =
				aqe.execute(AdhocQuery.builder().measure(k1SumSquared.getName()).debug(true).build(), jooqDb);
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.keySet().map(AdhocSliceAsMap::getCoordinates).toList())
				.containsExactly(Map.of());
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1SumSquared.getName(), (long) Math.pow(123 + 234, 2)));
	}

	@Test
	public void testAdhocQuery_ByA() {
		dsl.execute("""
				CREATE TABLE someTableName AS
					SELECT 'a1' AS a, 123 AS k1 UNION ALL
					SELECT 'a2' AS a, 234 AS k1
					;
				""");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		AdhocMeasureBag measureBag = AdhocMeasureBag.builder().build();
		measureBag.addMeasure(k1Sum);
		measureBag.addMeasure(k1SumSquared);

		AdhocQueryEngine aqe =
				AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()).measureBag(measureBag).build();

		ITabularView result =
				aqe.execute(AdhocQuery.builder().measure(k1SumSquared.getName()).groupByAlso("a").debug(true).build(),
						jooqDb);
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.keySet().map(AdhocSliceAsMap::getCoordinates).toList())
				.contains(Map.of("a", "a1"), Map.of("a", "a2"));
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of(k1SumSquared.getName(), (long) Math.pow(123, 2)))
				.containsEntry(Map.of("a", "a2"), Map.of(k1SumSquared.getName(), (long) Math.pow(234, 2)));
	}

	@Test
	public void testAdhocQuery_FilterA1() {
		dsl.execute("""
				CREATE TABLE someTableName AS
					SELECT 'a1' AS a, 123 AS k1 UNION ALL
					SELECT 'a2' AS a, 234 AS k1
					;
				""");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		AdhocMeasureBag measureBag = AdhocMeasureBag.builder().build();
		measureBag.addMeasure(k1Sum);
		measureBag.addMeasure(k1SumSquared);

		AdhocQueryEngine aqe =
				AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()).measureBag(measureBag).build();

		ITabularView result = aqe.execute(
				AdhocQuery.builder().measure(k1SumSquared.getName()).andFilter("a", "a1").debug(true).build(),
				jooqDb);
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.keySet().map(AdhocSliceAsMap::getCoordinates).toList()).contains(Map.of());
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1SumSquared.getName(), (long) Math.pow(123, 2)));
	}

	// https://stackoverflow.com/questions/361747/what-does-the-symbol-do-in-sql
	@Test
	public void testAdhocQuery_FilterA1_groupByB_columnWithAtSymbol() {
		dsl.execute("""
				CREATE TABLE someTableName AS
					SELECT 'a1' AS "a@a@a", 'b1' AS "b@b@b", 123 AS k1 UNION ALL
					SELECT 'a2' AS "a@a@a", 'b2' AS "b@b@b", 234 AS k1 UNION ALL
					SELECT 'a1' AS "a@a@a", 'b2' AS "b@b@b", 345 AS k1
					;
				""");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		AdhocMeasureBag measureBag = AdhocMeasureBag.builder().build();
		measureBag.addMeasure(k1Sum);
		measureBag.addMeasure(k1SumSquared);

		AdhocQueryEngine aqe =
				AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()).measureBag(measureBag).build();

		ITabularView result = aqe.execute(AdhocQuery.builder()
				.measure(k1SumSquared.getName())
				.andFilter("a@a@a", "a1")
				.groupByAlso("b@b@b")
				.debug(true)
				.build(), jooqDb);
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.keySet().map(AdhocSliceAsMap::getCoordinates).toList())
				.contains(Map.of("b@b@b", "b1"), Map.of("b@b@b", "b2"));
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("b@b@b", "b1"), Map.of(k1SumSquared.getName(), (long) Math.pow(123, 2)))
				.containsEntry(Map.of("b@b@b", "b2"), Map.of(k1SumSquared.getName(), (long) Math.pow(345, 2)));
	}
}
