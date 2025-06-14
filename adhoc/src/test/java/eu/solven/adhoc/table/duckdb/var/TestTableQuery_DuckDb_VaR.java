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

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.duckdb.DuckDBConnection;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocTestHelper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.context.GeneratedColumnsPreparator;
import eu.solven.adhoc.engine.context.IQueryPreparator;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import eu.solven.pepper.memory.IPepperMemoryConstants;
import eu.solven.pepper.memory.PepperMemoryHelper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestTableQuery_DuckDb_VaR extends ADagTest implements IAdhocTestConstants, IExampleVaRConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	static int maxCardinality = 10_000;
	static int arrayLength = 20;

	@BeforeAll
	public static void logAboutDatasetSize() {
		log.info("Considering VaR over {}",
				PepperMemoryHelper.memoryAsString(IPepperMemoryConstants.DOUBLE * arrayLength * maxCardinality));
	}

	String tableName = "someTableName";

	DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();
	JooqTableWrapper table = new JooqTableWrapper(tableName,
			JooqTableWrapperParameters.builder()
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

	private CubeWrapper wrapInCube(IMeasureForest forest) {
		CubeQueryEngine engine = CubeQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

		return CubeWrapper.builder()
				.engine(engine)
				.forest(forest)
				.table(table)
				.queryPreparator(customQueryPreparator())
				.build();
	}

	private IQueryPreparator customQueryPreparator() {
		return GeneratedColumnsPreparator.builder().generatedColumnsMeasure(mArray).build();
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

			// https://github.com/duckdb/duckdb-java/issues/163
			// doInsertSpecificArrays(duckDbConnection);

			s.execute("""
					INSERT INTO someTableName (
					    SELECT
					        if(random() > 0.5, 'red', 'blue'),
					        list_transform(range(%s), x -> RANDOM()::DECIMAL(2, 1))::DOUBLE[]
					    FROM range(%s)
					);""".formatted(arrayLength, maxCardinality));
		});

		registerMeasures();
	}

	private void registerMeasures() {
		List<String> underlyings = new ArrayList<>();

		forest.addMeasure(countAsterisk);

		// DuckDB does not enable array aggregation: we aggregate each index individually
		for (int i = 0; i < arrayLength; i++) {
			String name = "k" + i;

			underlyings.add(name);
			forest.addMeasure(Aggregator.builder()
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
					.combinationOptions(Map.of(NB_SCENARIO, arrayLength))
					.build();
			forest.addMeasure(k1ArrayFromPrimitives);

			k1Array = Dispatchor.builder()
					.name(mArray)
					.underlying(k1ArrayFromPrimitives.getName())
					.decompositionKey(ExampleVaRDecomposition.class.getName())
					.decompositionOptions(Map.of(NB_SCENARIO, arrayLength))
					.aggregationKey(CoalesceAggregation.class.getName())
					.build();
			forest.addMeasure(k1Array);

			// This measure enables accessing each array index by a functional name
			Combinator k1ScenariosNames = Combinator.builder()
					.name(mNames)
					.underlying(mArray)
					.combinationKey(ExampleVaRScenarioNameCombination.class.getName())
					.combinationOptions(Map.of(NB_SCENARIO, arrayLength))
					.build();
			forest.addMeasure(k1ScenariosNames);
		}
		IMeasure k1VaR = Combinator.builder()
				.name(mVaR)
				.underlying(mArray)
				.combinationKey(ExampleVaRQuickSelectCombination.class.getName())
				.combinationOptions(ImmutableMap.<String, Object>builder()
						.put(ExampleVaRQuickSelectCombination.P_QUANTILE, 0.95D)
						.build())
				.build();
		forest.addMeasure(k1VaR);
	}

	@Test
	public void testGrandTotal() {
		ITabularView result =
				wrapInCube(forest).execute(CubeQuery.builder().measure(countAsterisk.getName(), mArray, mVaR).build());
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
		ITabularView result = wrapInCube(forest).execute(
				CubeQuery.builder().measure(countAsterisk.getName(), mArray, mVaR).groupByAlso("color").build());
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

	// groupBy scenarioIndex
	@Test
	public void testGroupByScenarioIndex_mArray() {
		ITabularView result =
				wrapInCube(forest).execute(CubeQuery.builder().measure(mArray).groupByAlso(C_SCENARIOINDEX).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsKeys(Map.of(C_SCENARIOINDEX, 0), Map.of(C_SCENARIOINDEX, arrayLength - 1))
				.hasSize(arrayLength);

		for (int scenarioIndex : IntStream.range(0, arrayLength).toArray()) {
			Map<String, ?> measures = mapBased.getCoordinatesToValues().get(Map.of(C_SCENARIOINDEX, scenarioIndex));
			Assertions.assertThat(measures).hasSize(1);

			Object array = measures.get(mArray);
			Assertions.assertThat(array).isInstanceOf(Long.class);
		}
	}

	// groupBy scenarioName
	@Test
	public void testGroupByScenarioName_mArray() {
		ITabularView result =
				wrapInCube(forest).execute(CubeQuery.builder().measure(mArray).groupByAlso(C_SCENARIONAME).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsKeys(Map.of(C_SCENARIONAME, "histo_" + 0),
						Map.of(C_SCENARIONAME, "histo_" + (arrayLength - 1)))
				.hasSize(arrayLength);

		for (int scenarioIndex : IntStream.range(0, arrayLength).toArray()) {
			Map<String, ?> measures = mapBased.getCoordinatesToValues()
					.get(Map.of(C_SCENARIONAME, ExampleVaRScenarioNameCombination.indexToName(scenarioIndex)));
			Assertions.assertThat(measures).hasSize(1);

			Object array = measures.get(mArray);
			Assertions.assertThat(array).isInstanceOf(Long.class);
		}
	}

	// filter scenarioIndex
	@Test
	public void testFilterScenarioIndex_mArrayVaR() {
		ITabularView result = wrapInCube(forest)
				.execute(CubeQuery.builder().measure(mArray, mVaR).andFilter(C_SCENARIOINDEX, 0).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).containsKey(Map.of()).hasSize(1);

		Map<String, ?> measureToValue = mapBased.getCoordinatesToValues().get(Map.of());

		Assertions.assertThat(measureToValue).hasSize(2).containsKeys(mArray, mVaR);

		Assertions.assertThat(measureToValue.get(mVaR)).isInstanceOf(Long.class);
		Assertions.assertThat(measureToValue.get(mArray)).isInstanceOfSatisfying(int[].class, intArray -> {
			Assertions.assertThat(intArray).hasSize(1).contains(((Long) measureToValue.get(mVaR)).intValue());
		});
	}

	// filter scenarioName
	@Test
	public void testFilterScenarioName_mArrayVaR() {
		ITabularView result = wrapInCube(forest).execute(CubeQuery.builder()
				.measure(mArray, mVaR)
				.andFilter(C_SCENARIONAME, ExampleVaRScenarioNameCombination.indexToName(0))
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

	// filter scenarioIndex on `count(*)`: the filter is suppressed
	@Test
	public void testFilterScenarioIndex_countAsterisk_standardCube() {
		ITabularView result = wrapInCube(forest)
				.execute(CubeQuery.builder().measure(countAsterisk).andFilter(C_SCENARIOINDEX, 0).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).containsKey(Map.of()).hasSize(1);

		Map<String, ?> measureToValue = mapBased.getCoordinatesToValues().get(Map.of());

		Assertions.assertThat(measureToValue).hasSize(1).containsKeys(countAsterisk.getName());

		Assertions.assertThat(measureToValue.get(countAsterisk.getName())).isEqualTo(2L);
	}

	// groupBy scenarioIndex on `count(*)`: groupBy on `generated`
	@Test
	public void testGroupByScenarioIndex_countAsterisk() {
		ITabularView view = wrapInCube(forest)
				.execute(CubeQuery.builder().measure(countAsterisk).groupByAlso(C_SCENARIOINDEX).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(view);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsKey(Map.of(C_SCENARIOINDEX, IColumnGenerator.COORDINATE_GENERATED))
				.hasSize(1);

		Map<String, ?> measureToValue =
				mapBased.getCoordinatesToValues().get(Map.of(C_SCENARIOINDEX, IColumnGenerator.COORDINATE_GENERATED));

		Assertions.assertThat(measureToValue).hasSize(1).containsKeys(countAsterisk.getName());

		Assertions.assertThat(measureToValue.get(countAsterisk.getName())).isEqualTo(2L);
	}

	// groupBy and filter scenarioIndex on `count(*)`: groupBy on `generated`
	@Test
	public void testGroupByAndFilterScenarioIndex_countAsterisk() {
		ITabularView view = wrapInCube(forest).execute(CubeQuery.builder()
				.measure(countAsterisk)
				.groupByAlso(C_SCENARIOINDEX)
				.andFilter(C_SCENARIOINDEX, 0)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(view);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).isEmpty();
	}

	// filter scenarioIndex on empty measure
	@Test
	public void testGroupByScenario_emptyMeasure() {
		ITabularView result = wrapInCube(forest).execute(CubeQuery.builder().groupByAlso(C_SCENARIOINDEX).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsKey(Map.of(C_SCENARIOINDEX, "generated"))
				.hasSize(1);

		Map<String, ?> measureToValue = mapBased.getCoordinatesToValues().get(Map.of(C_SCENARIOINDEX, "generated"));
		Assertions.assertThat(measureToValue).isEmpty();
	}

	@Test
	public void testDescribe() {
		CubeWrapper cube = wrapInCube(forest);

		Assertions.assertThat(cube.getColumnTypes())
				.containsEntry("color", String.class)
				.containsEntry("0", Double.class)
				.containsEntry(Integer.toString(arrayLength - 1), Double.class)
				.containsEntry(C_SCENARIOINDEX, Integer.class)
				.containsEntry(C_SCENARIONAME, String.class)
				.hasSize(arrayLength
						// color
						+ 1
						// scenarioIndex and scenarioName
						+ 2);
	}

	@Test
	public void testColumnMeta_scenarioIndexes() {
		CubeWrapper cube = wrapInCube(forest);

		CoordinatesSample coordinates = cube.getCoordinates(C_SCENARIOINDEX, IValueMatcher.MATCH_ALL, arrayLength);

		Assertions.assertThat(coordinates.getEstimatedCardinality()).isEqualTo(arrayLength);
		Assertions.assertThat(coordinates.getCoordinates()).hasSize(arrayLength).contains(0, 1, arrayLength - 1);
	}

	@Test
	public void testColumnMeta_scenarioNames() {
		CubeWrapper cube = wrapInCube(forest);

		CoordinatesSample coordinates = cube.getCoordinates(C_SCENARIONAME, IValueMatcher.MATCH_ALL, arrayLength);

		Assertions.assertThat(coordinates.getEstimatedCardinality()).isEqualTo(arrayLength);
		Assertions.assertThat(coordinates.getCoordinates())
				.hasSize(arrayLength)
				.contains("histo_0", "histo_1", "histo_" + (arrayLength - 1));
	}

}
