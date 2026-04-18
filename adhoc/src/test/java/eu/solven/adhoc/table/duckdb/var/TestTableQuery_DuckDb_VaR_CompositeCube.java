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
package eu.solven.adhoc.table.duckdb.var;

import java.sql.Statement;
import java.sql.Wrapper;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.duckdb.DuckDBConnection;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.composite.CompositeCubesTableWrapper;
import eu.solven.adhoc.table.duckdb.ADuckDbJooqTest;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

/**
 * Combines the VaR example (calculated scenario hierarchy via {@link ExampleVaRDecomposition}) with a
 * {@link CompositeCubesTableWrapper}, to probe the interaction between a measure that does not participate in the
 * scenario hierarchy ({@code count(*)}) and that same hierarchy surfaced on the composite cube.
 *
 * The reference scenarios on a single (non-composite) cube are covered by
 * {@code TestTableQuery_DuckDb_VaR#testFilterScenarioIndex_countAsterisk_standardCube} and
 * {@code TestTableQuery_DuckDb_VaR#testGroupByScenarioIndex_countAsterisk}: the filter on {@code scenarioIndex} is
 * suppressed (count ignores the hierarchy) and a groupBy on {@code scenarioIndex} collapses the count onto the
 * {@link IColumnGenerator#COORDINATE_GENERATED} coordinate. This test class verifies that the composite cube preserves
 * the same semantics when two sub-cubes expose the calculated hierarchy AND a third sub-cube does not know the
 * calculated columns at all. The composite detects cross-cube calculated columns (see
 * {@code CompositeCubesTableWrapper#computeCrossCubeCalculatedColumns}) and, for sub-cubes that do not declare them,
 * auto-suppresses the filter and collapses the groupBy to {@link IColumnGenerator#COORDINATE_GENERATED} instead of the
 * cube-name fallback.
 *
 * @author Benoit Lacelle
 */
public class TestTableQuery_DuckDb_VaR_CompositeCube extends ADuckDbJooqTest
		implements IAdhocTestConstants, IExampleVaRConstants {

	// Small dataset: 3 scenarios (array length) × 2 rows per sub-cube.
	static final int arrayLength = 3;

	final String tableName1 = "varTable1";
	final String tableName2 = "varTable2";
	// Sub-cube with no calculated columns (no VaR hierarchy). The composite must auto-suppress filters/groupBys
	// referencing `scenarioIndex` / `scenarioName` for this cube, because those columns do not exist here.
	final String tableNameSimple = "simpleTable";

	final String mArray = "k1Array";
	final String mVaR = "k1VaR";

	@Override
	public ITableWrapper makeTable() {
		throw new UnsupportedOperationException("This test builds a CompositeCube over multiple sub-cubes");
	}

	/**
	 * Builds a `JooqTableWrapper` which PIVOTs the FLOAT[] column into one column per scenario index, matching the
	 * schema used by {@code TestTableQuery_DuckDb_VaR}.
	 */
	private JooqTableWrapper makeVarTable(String tableName) {
		return new JooqTableWrapper(tableName, DuckDBHelper.parametersBuilder(dslSupplier).table(DSL.table("""
				(
					PIVOT
					(
						SELECT color, index, SUM(doubles) AS doubles FROM (
							SELECT color, unnest(doubles) AS doubles, generate_subscripts(doubles, 1) - 1 AS index
							FROM %s
						) GROUP BY color, index
					) ON index USING SUM(doubles)
				)
				""".formatted(tableName))).build());
	}

	/**
	 * Creates the physical DuckDB table and inserts the given rows. Each row is a (color, array) pair, with the array
	 * length expected to match {@link #arrayLength}.
	 */
	private void createAndFeedPhysicalTable(String tableName, List<Map.Entry<String, float[]>> rows) {
		dslSupplier.getDSLContext().connection(c -> {
			DuckDBConnection duckDbConnection = ((Wrapper) c).unwrap(DuckDBConnection.class);
			try (Statement s = duckDbConnection.createStatement()) {
				s.execute("CREATE TABLE " + tableName + " (color VARCHAR, doubles FLOAT[]);");

				for (Map.Entry<String, float[]> row : rows) {
					float[] arr = row.getValue();
					StringBuilder arrayLiteral = new StringBuilder("[");
					for (int i = 0; i < arr.length; i++) {
						if (i > 0) {
							arrayLiteral.append(", ");
						}
						arrayLiteral.append(arr[i]);
					}
					arrayLiteral.append("]::FLOAT[]");

					s.execute("INSERT INTO " + tableName + " VALUES ('" + row.getKey() + "', " + arrayLiteral + ");");
				}
			}
		});
	}

	/**
	 * Builds a fresh measure forest mirroring {@code TestTableQuery_DuckDb_VaR#registerMeasures}: per-index SUM
	 * aggregators, a Combinator packing them into an int[], the {@link ExampleVaRDecomposition} Dispatchor exposing the
	 * scenario hierarchy, and the VaR quantile Combinator. {@code countAsterisk} is registered too so the sub-cubes can
	 * answer it.
	 */
	private UnsafeMeasureForest buildVarForest(String forestName) {
		UnsafeMeasureForest subForest = UnsafeMeasureForest.builder().name(forestName).build();
		subForest.addMeasure(countAsterisk);

		List<String> underlyings = new ArrayList<>();
		for (int i = 0; i < arrayLength; i++) {
			String name = "k" + i;
			underlyings.add(name);
			subForest.addMeasure(Aggregator.builder()
					.name(name)
					.columnName(Integer.toString(i))
					.aggregationKey(SumAggregation.KEY)
					.build());
		}

		Combinator k1ArrayFromPrimitives = Combinator.builder()
				.name(mArray + "fromPrimitives")
				.underlyings(underlyings)
				.combinationKey(ExampleVaRArrayCombination.class.getName())
				.combinationOptions(Map.of(NB_SCENARIO, arrayLength))
				.build();
		subForest.addMeasure(k1ArrayFromPrimitives);

		Dispatchor k1Array = Dispatchor.builder()
				.name(mArray)
				.underlying(k1ArrayFromPrimitives.getName())
				.decompositionKey(ExampleVaRDecomposition.class.getName())
				.decompositionOptions(Map.of(NB_SCENARIO, arrayLength))
				.aggregationKey(CoalesceAggregation.class.getName())
				.build();
		subForest.addMeasure(k1Array);

		IMeasure k1VaR = Combinator.builder()
				.name(mVaR)
				.underlying(mArray)
				.combinationKey(ExampleVaRQuickSelectCombination.class.getName())
				.combinationOptions(ImmutableMap.<String, Object>builder()
						.put(ExampleVaRQuickSelectCombination.P_QUANTILE, 0.95D)
						.build())
				.build();
		subForest.addMeasure(k1VaR);

		return subForest;
	}

	/**
	 * Creates a simple DuckDB table with schema {@code (color VARCHAR)} and inserts {@code nbRows} deterministic rows
	 * so {@code count(*)} on this sub-cube is stable.
	 */
	private void createAndFeedSimpleTable(String tableName, int nbRows) {
		dslSupplier.getDSLContext().connection(c -> {
			DuckDBConnection duckDbConnection = ((Wrapper) c).unwrap(DuckDBConnection.class);
			try (Statement s = duckDbConnection.createStatement()) {
				s.execute("CREATE TABLE " + tableName + " (color VARCHAR);");
				for (int i = 0; i < nbRows; i++) {
					s.execute("INSERT INTO " + tableName + " VALUES ('color_" + i + "');");
				}
			}
		});
	}

	/**
	 * Builds a plain {@link JooqTableWrapper} (no PIVOT) for the simple sub-cube.
	 */
	private JooqTableWrapper makeSimpleTable(String tableName) {
		return new JooqTableWrapper(tableName,
				DuckDBHelper.parametersBuilder(dslSupplier).tableName(tableName).build());
	}

	/**
	 * Forest for the simple sub-cube: just {@code count(*)}. Crucially, no {@link ExampleVaRDecomposition}, so this
	 * cube exposes neither {@code scenarioIndex} nor {@code scenarioName}.
	 */
	private UnsafeMeasureForest buildSimpleForest(String forestName) {
		UnsafeMeasureForest subForest = UnsafeMeasureForest.builder().name(forestName).build();
		subForest.addMeasure(countAsterisk);
		return subForest;
	}

	private CubeWrapper wrapInCube(IMeasureForest subForest, JooqTableWrapper table) {
		return CubeWrapper.builder()
				.name(table.getName() + ".cube")
				.engine(engine())
				.forest(subForest)
				.table(table)
				.build();
	}

	/**
	 * Number of rows in the simple sub-cube. Kept separate from the VaR sub-cubes so assertions can read as
	 * {@code 2 + 2 + SIMPLE_NB_ROWS} and the contribution of each sub-cube is obvious.
	 */
	private static final int SIMPLE_NB_ROWS = 3;

	/**
	 * Builds a composite cube over three sub-cubes: two VaR sub-cubes (exposing the {@code scenarioIndex} calculated
	 * hierarchy) and one simple sub-cube with no calculated columns at all. Each sub-cube is fed with a small
	 * deterministic dataset so {@code count(*)} assertions are stable.
	 */
	private CubeWrapper makeAndFeedCompositeCube() {
		createAndFeedPhysicalTable(tableName1,
				List.of(new AbstractMap.SimpleEntry<>("red", new float[] { 1.0f, 2.0f, 3.0f }),
						new AbstractMap.SimpleEntry<>("blue", new float[] { 4.0f, 5.0f, 6.0f })));
		createAndFeedPhysicalTable(tableName2,
				List.of(new AbstractMap.SimpleEntry<>("red", new float[] { 10.0f, 20.0f, 30.0f }),
						new AbstractMap.SimpleEntry<>("blue", new float[] { 40.0f, 50.0f, 60.0f })));
		createAndFeedSimpleTable(tableNameSimple, SIMPLE_NB_ROWS);

		CubeWrapper cube1 = wrapInCube(buildVarForest(tableName1), makeVarTable(tableName1));
		CubeWrapper cube2 = wrapInCube(buildVarForest(tableName2), makeVarTable(tableName2));
		CubeWrapper cubeSimple = wrapInCube(buildSimpleForest(tableNameSimple), makeSimpleTable(tableNameSimple));

		UnsafeMeasureForest compositeForest = UnsafeMeasureForest.builder().name("compositeVar").build();

		CompositeCubesTableWrapper compositeTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).cube(cubeSimple).build();
		IMeasureForest injectedForest = compositeTable.injectUnderlyingMeasures(compositeForest);

		return CubeWrapper.builder().engine(engine()).forest(injectedForest).table(compositeTable).build();
	}

	/**
	 * Sanity check: {@code count(*)} on the composite cube without any scenario interaction. Each VaR sub-cube PIVOTs
	 * to one row per distinct color ({@code count=2} each), and the simple sub-cube reports
	 * {@code count=SIMPLE_NB_ROWS}.
	 */
	@Test
	public void testCountAsterisk_grandTotal() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(CubeQuery.builder().measure(countAsterisk).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(countAsterisk.getName(), 0L + 2 + 2 + SIMPLE_NB_ROWS))
				.hasSize(1);
	}

	/**
	 * {@code count(*)} is an aggregator that does not participate in the scenario hierarchy. On the standard cube a
	 * filter on {@code scenarioIndex} is suppressed (see
	 * {@code TestTableQuery_DuckDb_VaR#testFilterScenarioIndex_countAsterisk_standardCube}). The composite cube
	 * preserves the same semantics for every sub-cube: the VaR sub-cubes suppress the filter internally (the column
	 * exists but is orthogonal to {@code count(*)}), and the simple sub-cube — which does not declare the column — has
	 * the filter auto-suppressed too because {@code scenarioIndex} is a cross-cube calculated column (declared as such
	 * by at least one other sub-cube). Totals are then summed.
	 */
	@Test
	public void testFilterScenarioIndex_countAsterisk_compositeCube() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3
				.execute(CubeQuery.builder().measure(countAsterisk).andFilter(C_SCENARIOINDEX, 0).debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).containsKey(Map.of()).hasSize(1);

		Map<String, ?> measureToValue = mapBased.getCoordinatesToValues().get(Map.of());
		Assertions.assertThat(measureToValue).hasSize(1).containsKey(countAsterisk.getName());
		Assertions.assertThat(measureToValue.get(countAsterisk.getName())).isEqualTo(0L + 2 + 2 + SIMPLE_NB_ROWS);
	}

	/**
	 * {@code count(*)} grouped by {@code scenarioIndex}. On the standard cube this returns a single row at
	 * {@link IColumnGenerator#COORDINATE_GENERATED} (see
	 * {@code TestTableQuery_DuckDb_VaR#testGroupByScenarioIndex_countAsterisk}). The composite cube emits the same
	 * single row: the VaR sub-cubes collapse to {@code scenarioIndex=generated} because {@code count(*)} is orthogonal
	 * to the scenario hierarchy, and the simple sub-cube — which does not declare the column — collapses to the same
	 * coordinate because {@code scenarioIndex} is a cross-cube calculated column.
	 */
	@Test
	public void testGroupByScenarioIndex_countAsterisk_compositeCube() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView view = cube3
				.execute(CubeQuery.builder().measure(countAsterisk).groupByAlso(C_SCENARIOINDEX).debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(view);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsKey(Map.of(C_SCENARIOINDEX, IColumnGenerator.COORDINATE_GENERATED))
				.hasSize(1);

		Map<String, ?> measureToValue =
				mapBased.getCoordinatesToValues().get(Map.of(C_SCENARIOINDEX, IColumnGenerator.COORDINATE_GENERATED));
		Assertions.assertThat(measureToValue).hasSize(1).containsKey(countAsterisk.getName());
		Assertions.assertThat(measureToValue.get(countAsterisk.getName())).isEqualTo(0L + 2 + 2 + SIMPLE_NB_ROWS);
	}
}
