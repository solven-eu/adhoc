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
package eu.solven.adhoc.table.duckdb.quantile;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.duckdb.DuckDBConnection;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.IAdhocMeasureBag;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapper;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.DuckDbHelper;

public class TestTableQuery_DuckDb_VaR extends ADagTest implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	String tableName = "someTableName";

	DSLSupplier jooqInMemorySupplier = DuckDbHelper.inMemoryDSLSupplier();
	AdhocJooqTableWrapper table = new AdhocJooqTableWrapper(tableName,
			AdhocJooqTableWrapperParameters.builder()
					.dslSupplier(jooqInMemorySupplier)
					// `generate_subscripts` is 1-based
					.table(DSL.table(
							"(PIVOT (SELECT color, index, SUM(doubles) AS doubles FROM (SELECT color, unnest(doubles) AS doubles, generate_subscripts(doubles, 1) - 1 AS index FROM someTableName) GROUP BY color, index) ON index USING SUM(doubles))"))
					.build());

	DSLContext dsl = table.makeDsl();

	private AdhocCubeWrapper wrapInCube(IAdhocMeasureBag measures) {
		AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

		return AdhocCubeWrapper.builder().engine(aqe).measures(measures).table(table).engine(aqe).build();
	}

	int arrayLength = 2;

	@Override
	@BeforeEach
	public void feedTable() {

		// https://duckdb.org/docs/stable/sql/data_types/array.html
		jooqInMemorySupplier.getDSLContext().connection(c -> {
			DuckDBConnection duckDbConnection = (DuckDBConnection) c;

			Statement s = duckDbConnection.createStatement();

			// We generally want to store FLOAT but aggregate as DOUBLE
			// No fixed size due to
			// https://github.com/duckdb/duckdb/issues/16672
			s.execute("CREATE TABLE someTableName (color VARCHAR, names STRING[], doubles FLOAT[]);"
					.formatted(arrayLength, arrayLength));

			// INSERT INTO someTableName VALUES ('red' , ['d0', 'd1'], [1.2, 2.3]);
			// INSERT INTO someTableName VALUES ('blue', ['d0', 'd1'], [3.4, 4.5]);
			// INSERT INTO someTableName VALUES ('blue', ['d0', 'd1'], [5.6, 6.7]);
			// SELECT unnest(array_values), generate_subscripts(array_values, 1) AS index FROM someTableName;
			// SELECT color, index, SUM(doubles) AS doubles FROM (SELECT color, unnest(array_values) AS doubles,
			// generate_subscripts(array_values, 1) AS index FROM someTableName) GROUP BY color, index ORDER BY index
			// ASC;
			// PIVOT (SELECT color, index, SUM(doubles) AS doubles FROM (SELECT color, unnest(array_values) AS doubles,
			// generate_subscripts(array_values, 1) AS index FROM someTableName) GROUP BY color, index ORDER BY index
			// ASC) ON index USING SUM(doubles);

			// duckDbConnection.createArrayOf(tableName, new double[] { 12.34 });

			// https://duckdb.org/docs/stable/clients/java.html
			// https://github.com/duckdb/duckdb-rs/issues/422
			try (var appender = duckDbConnection.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName)) {
				for (int rowIndex = 0; rowIndex < 16; rowIndex++) {

					double rowFactor = Math.sqrt(rowIndex);

					appender.beginRow();
					appender.append("red");
					appender.append(IntStream.range(0, arrayLength)
							.mapToObj(i -> "d" + i)
							.collect(Collectors.joining(", ", "[", "]")));
					appender.append(IntStream.range(0, arrayLength)
							.mapToDouble(i -> rowFactor * Math.sqrt(i))
							.mapToObj(d -> Double.toString(d))
							.collect(Collectors.joining(", ", "[", "]")));
					appender.endRow();
				}
			}

			// Arrow binding
			// try (var allocator = new RootAllocator();
			// ArrowStreamReader reader = null; // should not be null of course
			// var arrow_array_stream = ArrowArrayStream.allocateNew(allocator)) {
			// Data.exportArrayStream(allocator, reader, arrow_array_stream);
			//
			// // DuckDB setup
			// try (var conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
			// conn.registerArrowStream("asdf", arrow_array_stream);
			//
			// // run a query
			// try (var stmt = conn.createStatement();
			// var rs = (DuckDBResultSet) stmt.executeQuery("SELECT count(*) FROM asdf")) {
			// while (rs.next()) {
			// System.out.println(rs.getInt(1));
			// }
			// }
			// }
			// }

			// https://duckdb.org/docs/stable/clients/adbc.html
			// final Map<String, Object> parameters = new HashMap<>();
			// AdbcDriver.PARAM_URI.set(parameters, "jdbc:postgresql://localhost:5432/postgres");
			// try (BufferAllocator allocator = new RootAllocator();
			// AdbcDatabase db = new JdbcDriver(allocator).open(parameters);
			// AdbcConnection adbcConnection = db.connect();
			// AdbcStatement stmt = adbcConnection.createStatement()
			//
			// ) {
			// stmt.setSqlQuery("select * from foo");
			// AdbcStatement.QueryResult queryResult = stmt.executeQuery();
			// while (queryResult.getReader().loadNextBatch()) {
			// // process batch
			// }
			// } catch (AdbcException e) {
			// // throw
			// }
		});
	}

	@Test
	public void testWholeQuery() {
		List<String> underlyings = new ArrayList<>();
		for (int i = 0; i < arrayLength; i++) {
			String name = "k" + i;

			underlyings.add(name);
			amb.addMeasure(Aggregator.builder()
					.name(name)
					.columnName(Integer.toString(i))
					.aggregationKey(SumAggregation.KEY)
					.build());
		}

		{

			amb.addMeasure(Combinator.builder()
					.name("k1Array")
					.underlyings(underlyings)
					.combinationKey(CustomArrayCombination.class.getName())
					.build());
		}
		IMeasure k1VaR = Combinator.builder()
				.name("k1VaR")
				.underlying("k1Array")
				.combinationKey(CustomVaRCombination.class.getName())
				.combinationOptions(
						ImmutableMap.<String, Object>builder().put(CustomVaRCombination.P_QUANTILE, 0.95D).build())
				.build();
		amb.addMeasure(k1VaR);

		ITabularView result = wrapInCube(amb).execute(AdhocQuery.builder().measure(k1VaR.getName()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).containsEntry(Map.of(), Map.of(k1VaR.getName(), 40L));
	}

	@Test
	public void testDescribe() {
		AdhocCubeWrapper cube = wrapInCube(amb);

		Assertions.assertThat(cube.getColumns())
				.containsEntry("color", String.class)
				.containsEntry("0", Double.class)
				.containsEntry(Integer.toString(arrayLength - 1), Double.class)
				.hasSize(arrayLength + 1);
	}
}
