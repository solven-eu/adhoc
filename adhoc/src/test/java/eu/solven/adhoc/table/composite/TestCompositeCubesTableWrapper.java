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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Phaser;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.UnsafeMeasureForestBag;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.table.composite.PhasedTableWrapper.TableWrapperPhasers;
import smile.math.MathEx;

public class TestCompositeCubesTableWrapper implements IAdhocTestConstants {

	private CubeWrapper wrapInCube(IMeasureForest forest, ITableWrapper table) {
		AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

		return CubeWrapper.builder()
				.name(table.getName() + ".cube")
				.engine(aqe)
				.forest(forest)
				.table(table)
				.engine(aqe)
				.build();
	}

	// `k1` is both an underdlyingCube measure, and an explicit cube measure.
	@Test
	public void testAddUnderlyingMeasures_sameMeasurenameInUnderlyingAndInComposite() {
		Aggregator k3Max = Aggregator.builder().name("k3").aggregationKey(MaxAggregation.KEY).build();

		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

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

		UnsafeMeasureForestBag withoutUnderlyings = UnsafeMeasureForestBag.builder().name("composite").build();

		// In the composite cube, we want `k1` to be the maximum through cubes
		Aggregator k1Max = k1Sum.toBuilder().aggregationKey(MaxAggregation.KEY).build();
		// Check there is a conflict on the measure name
		Assertions.assertThat(k1Max.getName()).isEqualTo(k1Sum.getName());

		withoutUnderlyings.addMeasure(k1Max);
		withoutUnderlyings.addMeasure(k1PlusK2AsExpr);

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		IMeasureForest withUnderlyings = compositeCubesTable.injectUnderlyingMeasures(withoutUnderlyings);

		Assertions.assertThat(withUnderlyings.getNameToMeasure().values())
				.hasSize(6)
				// Composite own measures
				.contains(k1Max, k1PlusK2AsExpr)

				// Cube1 measures, available through composite
				.contains(SubMeasureAsAggregator.builder()
						// k1 is both a compositeMeasure and a cube1 measure
						// We aliased the underlyingMeasure
						.name(k1Sum.getName() + "." + tableName1 + ".cube")
						.subMeasure(k1Sum.getName())
						.aggregationKey(SumAggregation.KEY)
						.build())
				.contains(SubMeasureAsAggregator.builder()
						// k2 is only a cube1 measure
						// Not aliased
						.name(k2Sum.getName())
						.subMeasure(k2Sum.getName())
						.aggregationKey(SumAggregation.KEY)
						.build())

				// Cube2 measures, available through composite
				.contains(SubMeasureAsAggregator.builder()
						.name(k1Sum.getName() + "." + tableName2 + ".cube")
						.subMeasure(k1Sum.getName())
						.aggregationKey(SumAggregation.KEY)
						.build())
				.contains(SubMeasureAsAggregator.builder()
						.name(k3Max.getName())
						.subMeasure(k3Max.getName())
						.aggregationKey(MaxAggregation.KEY)
						.build());

		// Test an actual query result
		{
			table1.add(Map.of("k1", 123));
			table1.add(Map.of("k1", 234));

			table2.add(Map.of("k1", 345));
			table2.add(Map.of("k1", 456));

			CubeWrapper compositeCube =
					CubeWrapper.builder().name("composite").table(compositeCubesTable).forest(withUnderlyings).build();

			ITabularView view = compositeCube.execute(AdhocQuery.builder().measure(k1Max).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(view);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1Max.getName(), 0L + MathEx.max(123, 234, 345, 456)))
					.hasSize(1);
		}
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

	// This test will ensure 2 underlying tables are queried concurrently
	@Test
	public void testConcurrency() {
		TableWrapperPhasers phasers = TableWrapperPhasers.parties(2);

		String tableName1 = "someTableName1";
		ITableWrapper table1 = PhasedTableWrapper.builder().name(tableName1).phasers(phasers).build();

		String tableName2 = "someTableName2";
		ITableWrapper table2 = PhasedTableWrapper.builder().name(tableName2).phasers(phasers).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName1).build();
			measureBag.addMeasure(countAsterisk);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName2).build();
			measureBag.addMeasure(countAsterisk);
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForestBag withoutUnderlyings = UnsafeMeasureForestBag.builder().name("composite").build();

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		CubeWrapper cube = CubeWrapper.builder().table(compositeCubesTable).forest(withoutUnderlyings).build();
		ITabularView view = cube.execute(AdhocQuery.builder()
				.measure(Aggregator.countAsterisk())
				.option(StandardQueryOptions.CONCURRENT)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(view);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(Aggregator.countAsterisk().getName(), 0L + 2))
				.hasSize(1);

		Phaser opening = phasers.getOpening();
		Assertions.assertThat(opening.getPhase()).isEqualTo(1);
		Assertions.assertThat(opening.getArrivedParties()).isEqualTo(0);

		Phaser streaming = phasers.getStreaming();
		Assertions.assertThat(streaming.getPhase()).isEqualTo(1);
		Assertions.assertThat(streaming.getArrivedParties()).isEqualTo(0);

		Phaser closing = phasers.getClosing();
		Assertions.assertThat(closing.getPhase()).isEqualTo(1);
		Assertions.assertThat(closing.getArrivedParties()).isEqualTo(0);
	}

	// When a cube does not know a column, and given column is grouped by, Adhoc behave like the cube has a static given
	// coordinate. This test filtering on given coordinate.
	@Test
	public void testFilterOnMissingColumn() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForestBag withoutUnderlyings = UnsafeMeasureForestBag.builder().name("composite").build();
		withoutUnderlyings.addMeasure(k1Sum);

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		{
			table1.add(Map.of("k1", 123, "a", "a1"));
			table1.add(Map.of("k1", 234, "a", "a2"));

			table2.add(Map.of("k1", 345));
			table2.add(Map.of("k1", 456));

			// Consider a column in one table but not the other
			Assertions.assertThat(table1.getColumnTypes()).containsKey("a");
			Assertions.assertThat(table2.getColumnTypes()).doesNotContainKey("a");

			CubeWrapper compositeCube = CubeWrapper.builder()
					.name("composite")
					.table(compositeCubesTable)
					.forest(withoutUnderlyings)
					.build();

			// groupBy:a
			String missingColumnCoordinate = tableName2 + ".cube";
			{
				ITabularView view =
						compositeCube.execute(AdhocQuery.builder().measure(k1Sum.getName()).groupByAlso("a").build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(mapBased.getCoordinatesToValues())
						.containsEntry(Map.of("a", "a1"), Map.of(k1Sum.getName(), 0L + 123))
						.containsEntry(Map.of("a", "a2"), Map.of(k1Sum.getName(), 0L + 234))
						.containsEntry(Map.of("a", missingColumnCoordinate), Map.of(k1Sum.getName(), 0L + 345 + 456))
						.hasSize(3);
			}

			// filter unknownCoordinate grandTotal
			{
				ITabularView view = compositeCube.execute(
						AdhocQuery.builder().measure(k1Sum.getName()).andFilter("a", missingColumnCoordinate).build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(mapBased.getCoordinatesToValues())
						.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 345 + 456))
						.hasSize(1);
			}

			// filter unknownCoordinate groupBy:a
			{
				ITabularView view = compositeCube.execute(AdhocQuery.builder()
						.measure(k1Sum.getName())
						.andFilter("a", missingColumnCoordinate)
						.groupByAlso("a")
						.build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(mapBased.getCoordinatesToValues())
						.containsEntry(Map.of("a", missingColumnCoordinate), Map.of(k1Sum.getName(), 0L + 345 + 456))
						.hasSize(1);
			}
		}
	}

	@Test
	public void testCubeSlicer_onByDefault() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			cube2 = wrapInCube(measureBag, table2);
		}

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		Assertions.assertThat(compositeCubesTable.getColumnTypes()).containsKey("cubeSlicer");
	}

	@Test
	public void testCubeSlicer() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForestBag withoutUnderlyings = UnsafeMeasureForestBag.builder().name("composite").build();
		withoutUnderlyings.addMeasure(k1Sum);

		CompositeCubesTableWrapper compositeCubesTable = CompositeCubesTableWrapper.builder()
				.cube(cube1)
				.cube(cube2)
				.optCubeSlicer(Optional.of("cubeSlicer"))
				.build();

		{
			table1.add(Map.of("k1", 123, "a", "a1"));
			table1.add(Map.of("k1", 234, "a", "a2"));

			table2.add(Map.of("k1", 345));
			table2.add(Map.of("k1", 456));

			// Consider a column in one table but not the other
			Assertions.assertThat(compositeCubesTable.getColumnTypes()).containsKey("cubeSlicer");

			CubeWrapper compositeCube = CubeWrapper.builder()
					.name("composite")
					.table(compositeCubesTable)
					.forest(withoutUnderlyings)
					.build();

			// groupBy:cubeSlicer
			String m = k1Sum.getName();
			{
				ITabularView view =
						compositeCube.execute(AdhocQuery.builder().measure(m).groupByAlso("cubeSlicer").build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(mapBased.getCoordinatesToValues())
						.containsEntry(Map.of("cubeSlicer", tableName1 + ".cube"), Map.of(m, 0L + 123 + 234))
						.containsEntry(Map.of("cubeSlicer", tableName2 + ".cube"), Map.of(m, 0L + 345 + 456))
						.hasSize(2);
			}

			// groupBy:cubeSlicer&a
			{
				ITabularView view =
						compositeCube.execute(AdhocQuery.builder().measure(m).groupByAlso("cubeSlicer", "a").build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(mapBased.getCoordinatesToValues())
						.containsEntry(Map.of("a", "a1", "cubeSlicer", tableName1 + ".cube"), Map.of(m, 0L + 123))
						.containsEntry(Map.of("a", "a2", "cubeSlicer", tableName1 + ".cube"), Map.of(m, 0L + 234))
						.containsEntry(Map.of("a", tableName2 + ".cube", "cubeSlicer", tableName2 + ".cube"),
								Map.of(m, 0L + 345 + 456))
						.hasSize(3);
			}
		}
	}
}
