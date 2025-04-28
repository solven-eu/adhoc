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
package eu.solven.adhoc.table.duckdb;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.map.MapTestHelpers;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;

public class TestTableQuery_DuckDb_FromParquet extends ADuckDbJooqTest implements IAdhocTestConstants {
	Path tmpParquetPath;
	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();

	DSLContext dsl;
	JooqTableWrapper table;

	@BeforeEach
	public void initParquetFiles() throws IOException {
		tmpParquetPath = Files.createTempFile(this.getClass().getSimpleName(), ".parquet");

		String tableName = "%s".formatted(tmpParquetPath.toAbsolutePath());

		Connection dbConn = makeFreshInMemoryDb();
		table = new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder()
						.dslSupplier(DSLSupplier.fromConnection(() -> dbConn))
						.tableName(tableName)
						.build());

		dsl = table.makeDsl();
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
		Assertions.assertThatThrownBy(() -> table.streamSlices(qK1).toList()).isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testTableDoesNotExists() throws IOException {
		Files.delete(tmpParquetPath);

		Assertions.assertThatThrownBy(() -> table.streamSlices(qK1).toList())
				.isInstanceOf(DataAccessException.class)
				.hasMessageContaining("IO Error: No files found that match the pattern");
	}

	@Test
	public void testGetColumns_TableDoesNotExists() throws IOException {
		Files.delete(tmpParquetPath);

		// This should not throw not to prevent Pivotable from loading
		Assertions.assertThat(table.getColumnTypes()).isEmpty();
	}

	@Test
	public void testEmptyDb() {
		dsl.execute("CREATE OR REPLACE TABLE someTableName (k1 DECIMAL);");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		List<Map<String, ?>> dbStream = table.streamSlices(qK1).toList();

		// It seems a legal SQL behavior: a groupBy with `null` is created even if there is not a single matching row
		Assertions.assertThat(dbStream).contains(MapTestHelpers.mapWithNull("k1")).hasSize(1);
	}

	@Test
	public void testReturnAll() {
		dsl.execute("CREATE TABLE someTableName AS SELECT 123 AS k1;");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		List<Map<String, ?>> dbStream = table.streamSlices(qK1).toList();

		Assertions.assertThat(dbStream).hasSize(1).contains(Map.of("k1", BigDecimal.valueOf(123)));
	}

	@Test
	public void testSumOnVarchar() {
		dsl.execute("CREATE TABLE someTableName AS SELECT 'someKey' AS k1;");
		dsl.execute("COPY someTableName TO '%s' (FORMAT PARQUET);".formatted(tmpParquetPath));

		Assertions.assertThatThrownBy(() -> table.streamSlices(qK1).toList()).isInstanceOf(DataAccessException.class);
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

		forest.addMeasure(k1Sum);
		forest.addMeasure(k1SumSquared);

		ITabularView result = aqe.executeUnsafe(AdhocQuery.builder().measure(k1SumSquared).build(), forest, table);
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

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

		forest.addMeasure(k1Sum);
		forest.addMeasure(k1SumSquared);

		ITabularView result =
				aqe.executeUnsafe(AdhocQuery.builder().measure(k1SumSquared).groupByAlso("a").build(), forest, table);
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

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

		forest.addMeasure(k1Sum);
		forest.addMeasure(k1SumSquared);

		ITabularView result =
				aqe.executeUnsafe(AdhocQuery.builder().measure(k1SumSquared.getName()).andFilter("a", "a1").build(),
						forest,
						table);
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

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

		forest.addMeasure(k1Sum);
		forest.addMeasure(k1SumSquared);

		ITabularView result = aqe.executeUnsafe(AdhocQuery.builder()
				.measure(k1SumSquared.getName())
				.andFilter("a@a@a", "a1")
				.groupByAlso("b@b@b")
				.build(), forest, table);
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("b@b@b", "b1"), Map.of(k1SumSquared.getName(), (long) Math.pow(123, 2)))
				.containsEntry(Map.of("b@b@b", "b2"), Map.of(k1SumSquared.getName(), (long) Math.pow(345, 2)));
	}
}
