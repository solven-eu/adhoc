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
package eu.solven.adhoc.table.composite;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.UnsafeMeasureForestBag;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;

public class TestCompositeCubesTableWrapper implements IAdhocTestConstants {

	private CubeWrapper wrapInCube(IMeasureForest forest, JooqTableWrapper table) {
		AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

		return CubeWrapper.builder()
				.name(table.getName() + ".cube")
				.engine(aqe)
				.forest(forest)
				.table(table)
				.engine(aqe)
				.build();
	}

	@Test
	public void testAddUnderlyingMeasures_sameMeasurenameInUnderlyingAndInComposite() {
		Aggregator k3Max = Aggregator.builder().name("k3").aggregationKey(MaxAggregation.KEY).build();

		DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();

		String tableName1 = "someTableName1";
		JooqTableWrapper table1 = new JooqTableWrapper(tableName1,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName1).build());

		String tableName2 = "someTableName2";
		JooqTableWrapper table2 = new JooqTableWrapper(tableName2,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName2).build());

		CubeWrapper cube1;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k2Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k3Max);
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForestBag measuresWithoutUnderlyings = UnsafeMeasureForestBag.builder().name("composite").build();
		measuresWithoutUnderlyings.addMeasure(k1Sum);
		measuresWithoutUnderlyings.addMeasure(k1PlusK2AsExpr);

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		IMeasureForest withUnderlyings = compositeCubesTable.injectUnderlyingMeasures(measuresWithoutUnderlyings);

		Assertions.assertThat(withUnderlyings.getNameToMeasure().values())
				.hasSize(6)
				// Composite own measures
				.contains(k1Sum, k1PlusK2AsExpr)

				// Cube1 measures, available through composite
				.contains(Aggregator.builder()
						// k1 is both a compositeMeasure and a cube1 measure
						// We aliased the underlyingMeasure
						.name(k1Sum.getName() + "." + tableName1 + ".cube")
						.aggregationKey(SumAggregation.KEY)
						.build())
				.contains(Aggregator.builder()
						// k2 is only a cube1 measure
						// Not aliased
						.name(k2Sum.getName())
						.aggregationKey(SumAggregation.KEY)
						.build())

				// Cube2 measures, available through composite
				.contains(Aggregator.builder()
						.name(k1Sum.getName() + "." + tableName2 + ".cube")
						.aggregationKey(SumAggregation.KEY)
						.build())
				.contains(Aggregator.builder()
						.name(k3Max.getName())
						// k3 is a MAX, but it is fed by a single cube: SUM is KO
						// BEWARE: if k3.MAX was provided by multiple cubes, we should probably prefer MAX in composite
						// aggregator
						.aggregationKey(SumAggregation.KEY)
						.build());
	}

	@Test
	public void testFilterUnderlyingCube() {
		CompositeCubesTableWrapper composite = CompositeCubesTableWrapper.builder().build();

		Assertions.assertThat(composite.filterForColumns(IAdhocFilter.MATCH_ALL, Set.of()))
				.isEqualTo(IAdhocFilter.MATCH_ALL);
		Assertions.assertThat(composite.filterForColumns(IAdhocFilter.MATCH_NONE, Set.of()))
				.isEqualTo(IAdhocFilter.MATCH_NONE);

		// and
		Assertions.assertThat(composite.filterForColumns(
				AndFilter.and(ColumnFilter.isLike("c1", "a%"), ColumnFilter.isLike("c2", "b%")),
				Set.of("c1"))).isEqualTo(ColumnFilter.isLike("c1", "a%"));
		// or
		Assertions.assertThat(composite.filterForColumns(
				OrFilter.or(ColumnFilter.isLike("c1", "a%"), ColumnFilter.isLike("c2", "b%")),
				Set.of("c1"))).isEqualTo(ColumnFilter.isLike("c1", "a%"));
	}
}
