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
package eu.solven.adhoc.table.duckdb.var;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.FirstNotNullAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
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

	int maxCardinality = 100_000;
	int arrayLength = 200;

	String tableName = "someTableName";

	DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();
	AdhocJooqTableWrapper table = new AdhocJooqTableWrapper(tableName,
			AdhocJooqTableWrapperParameters.builder()
					.dslSupplier(dslSupplier)
					// `generate_subscripts` is 1-based
					.table(DSL
							.table("""
									(
										PIVOT
										(
											SELECT color, index, SUM(doubles) AS doubles FROM (
												SELECT color, unnest(doubles) AS doubles, generate_subscripts(doubles, 1) - 1 AS index
												FROM someTableName
											) GROUP BY color, index
										) ON index USING SUM(doubles)
									)
									"""))
					.build());

	DSLContext dsl = table.makeDsl();

	private AdhocCubeWrapper wrapInCube(IMeasureForest forest) {
		AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

		return AdhocCubeWrapper.builder().engine(aqe).forest(forest).table(table).engine(aqe).build();
	}

	String mArray = "k1Array";
	String mNames = "arrayNames";
	String mVaR = "k1VaR";

	@Override
	@BeforeEach
	public void feedTable() {

		// https://duckdb.org/docs/stable/sql/data_types/array.html
		dslSupplier.getDSLContext().connection(c -> {
			DuckDBConnection duckDbConnection = (DuckDBConnection) c;

			Statement s = duckDbConnection.createStatement();

			// We generally want to store FLOAT but aggregate as DOUBLE
			// No fixed size due to
			// https://github.com/duckdb/duckdb/issues/16672
			s.execute(
					"CREATE TABLE someTableName (color VARCHAR, doubles FLOAT[]);".formatted(arrayLength, arrayLength));

			// doInsertSpecificArrays(duckDbConnection);

			s.execute("""
					INSERT INTO someTableName (
					    SELECT
					        if(random() > 0.5, 'red', 'blue'),
					        list_transform(range(%s), x -> RANDOM()::DECIMAL(2, 1))::DOUBLE[]
					    FROM range(%s)
					);
										""".formatted(arrayLength, maxCardinality));
		});

		registerMeasures();
	}

	private void registerMeasures() {
		List<String> underlyings = new ArrayList<>();

		amb.addMeasure(countAsterisk);

		// DuckDB does not enable array aggregation: we aggregate each index individually
		for (int i = 0; i < arrayLength; i++) {
			String name = "k" + i;

			underlyings.add(name);
			amb.addMeasure(Aggregator.builder()
					.name(name)
					.columnName(Integer.toString(i))
					.aggregationKey(SumAggregation.KEY)
					.build());
		}

		Dispatchor k1Array;
		{
			// This measure collects the N underlying DOUBLE measures into a double[] measure
			// Note that some DB may provide the array natively
			Combinator k1ArrayFromPrimitives = Combinator.builder()
					.name(mArray + "fromPrimitives")
					.underlyings(underlyings)
					.combinationKey(ExampleVaRArrayCombination.class.getName())
					.combinationOptions(Map.of(IExampleVaRConstants.NB_SCENARIO, arrayLength))
					.build();
			amb.addMeasure(k1ArrayFromPrimitives);

			k1Array = Dispatchor.builder()
					.name(mArray)
					.underlying(k1ArrayFromPrimitives.getName())
					.decompositionKey(ExampleVaRDecomposition.class.getName())
					.decompositionOptions(Map.of(IExampleVaRConstants.NB_SCENARIO, arrayLength))
					.aggregationKey(FirstNotNullAggregation.class.getName())
					.build();
			amb.addMeasure(k1Array);

			// This measure enables accessing each array index by a functional name
			Combinator k1ScenariosNames = Combinator.builder()
					.name(mNames)
					.underlying(mArray)
					.combinationKey(ExampleVaRScenarioNameCombination.class.getName())
					.combinationOptions(Map.of(IExampleVaRConstants.NB_SCENARIO, arrayLength))
					.build();
			amb.addMeasure(k1ScenariosNames);
		}
		IMeasure k1VaR = Combinator.builder()
				.name(mVaR)
				.underlying(mArray)
				.combinationKey(ExampleVaRCombination.class.getName())
				.combinationOptions(
						ImmutableMap.<String, Object>builder().put(ExampleVaRCombination.P_QUANTILE, 0.95D).build())
				.build();
		amb.addMeasure(k1VaR);
	}

	@Test
	public void testGrandTotal() {
		ITabularView result = wrapInCube(amb)
				.execute(AdhocQuery.builder().measure(countAsterisk.getName(), mArray, mVaR).debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).containsKey(Map.of()).hasSize(1);

		Map<String, ?> measures = mapBased.getCoordinatesToValues().get(Map.of());
		Assertions.assertThat(measures).hasSize(3);

		// TODO `Count(*)` is not the expected value. We would expect maxCardinality
		Assertions.assertThat(measures.get(countAsterisk.getName())).isEqualTo(0L + Set.of("blue", "red").size());

		Object array = measures.get(mArray);
		Assertions.assertThat(array).isInstanceOfSatisfying(int[].class, intArray -> {
			Assertions.assertThat(intArray).hasSize(arrayLength);

			long expectedQuantile = IntStream.of(intArray)
					.sorted()
					.skip((long) (arrayLength * 0.95))
					.mapToLong(i -> i)
					.findFirst()
					.getAsLong();

			Object quantile = measures.get(mVaR);
			Assertions.assertThat(quantile).isEqualTo(expectedQuantile);
		});
	}

	// groupBy standard column
	@Test
	public void testGroupByColor() {
		ITabularView result = wrapInCube(amb).execute(
				AdhocQuery.builder().measure(countAsterisk.getName(), mArray, mVaR).groupByAlso("color").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Set<String> colors = Set.of("blue", "red");

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsKeys(Map.of("color", "blue"), Map.of("color", "red"))
				.hasSize(colors.size());

		for (String color : colors) {
			Map<String, ?> measures = mapBased.getCoordinatesToValues().get(Map.of("color", color));
			Assertions.assertThat(measures).hasSize(3).containsKey(countAsterisk.getName());

			Object array = measures.get(mArray);
			Assertions.assertThat(array).isInstanceOfSatisfying(int[].class, intArray -> {
				Assertions.assertThat(intArray).hasSize(arrayLength);

				long expectedQuantile = IntStream.of(intArray)
						.sorted()
						.skip((long) (arrayLength * 0.95))
						.mapToLong(i -> i)
						.findFirst()
						.getAsLong();

				Object quantile = measures.get(mVaR);
				Assertions.assertThat(quantile).isEqualTo(expectedQuantile);
			});
		}
	}

	// groupBy arrayIndex
	@Test
	public void testGroupByIndex_mArray() {
		ITabularView result = wrapInCube(amb).execute(
				AdhocQuery.builder().measure(mArray).groupByAlso(IExampleVaRConstants.C_SCENARIOINDEX).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsKeys(Map.of(IExampleVaRConstants.C_SCENARIOINDEX, 0),
						Map.of(IExampleVaRConstants.C_SCENARIOINDEX, arrayLength - 1))
				.hasSize(arrayLength);

		for (int scenarioIndex : IntStream.range(0, arrayLength).toArray()) {
			Map<String, ?> measures =
					mapBased.getCoordinatesToValues().get(Map.of(IExampleVaRConstants.C_SCENARIOINDEX, scenarioIndex));
			Object array = measures.get(mArray);
			Assertions.assertThat(array).isInstanceOf(Long.class);
		}
	}

	// filter arrayIndex
	@Test
	public void testFilterIndex_mArrayVaR() {
		ITabularView result = wrapInCube(amb).execute(AdhocQuery.builder()
				.measure(mArray, mVaR)
				.andFilter(IExampleVaRConstants.C_SCENARIOINDEX, 0)
				.explain(true)
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).containsKey(Map.of()).hasSize(1);

		Map<String, ?> measureToValue = mapBased.getCoordinatesToValues().get(Map.of());

		Assertions.assertThat(measureToValue).hasSize(2).containsKeys(mArray, mVaR);

		Assertions.assertThat(measureToValue.get(mVaR)).isInstanceOf(Long.class);
		Assertions.assertThat(measureToValue.get(mArray)).isInstanceOfSatisfying(int[].class, intArray -> {
			Assertions.assertThat(intArray).hasSize(1).contains(((Long) measureToValue.get(mVaR)).intValue());
		});
	}

	@Test
	public void testDescribe() {
		AdhocCubeWrapper cube = wrapInCube(amb);

		Assertions.assertThat(cube.getColumns())
				.containsEntry("color", String.class)
				.containsEntry("0", Double.class)
				.containsEntry(Integer.toString(arrayLength - 1), Double.class)
				.containsEntry(IExampleVaRConstants.C_SCENARIOINDEX, Integer.class)
				.containsEntry(IExampleVaRConstants.C_SCENARIONAME, String.class)
				.hasSize(arrayLength
						// color
						+ 1
						// scenarioIndex and scenarioName
						+ 2);
	}

	@Test
	public void testColumnMeta_scenarioIndexes() {
		AdhocCubeWrapper cube = wrapInCube(amb);

		CoordinatesSample coordinates =
				cube.getCoordinates(IExampleVaRConstants.C_SCENARIOINDEX, IValueMatcher.MATCH_ALL, arrayLength);

		Assertions.assertThat(coordinates.getEstimatedCardinality()).isEqualTo(arrayLength);
		Assertions.assertThat(coordinates.getCoordinates()).hasSize(arrayLength).contains(0, 1, arrayLength - 1);
	}

	@Test
	public void testColumnMeta_scenarioNames() {
		AdhocCubeWrapper cube = wrapInCube(amb);

		CoordinatesSample coordinates =
				cube.getCoordinates(IExampleVaRConstants.C_SCENARIONAME, IValueMatcher.MATCH_ALL, arrayLength);

		Assertions.assertThat(coordinates.getEstimatedCardinality()).isEqualTo(arrayLength);
		Assertions.assertThat(coordinates.getCoordinates())
				.hasSize(arrayLength)
				.contains("histo_0", "histo_1", "histo_" + (arrayLength - 1));
	}

	// https://github.com/duckdb/duckdb-java/issues/163
	private void doInsertSpecificArrays(DuckDBConnection duckDbConnection) throws SQLException {
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
				// appender.append(scenarioNames());
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
	}
}
