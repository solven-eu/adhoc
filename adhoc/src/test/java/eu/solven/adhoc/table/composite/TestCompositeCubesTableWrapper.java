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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.ARawDagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.tabular.optimizer.CubeWrapperEditor;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.UnsafeMeasureForest;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.combination.EvaluatedExpressionCombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.table.composite.CompositeCubeHelper.CompatibleMeasures;
import eu.solven.adhoc.table.composite.PhasedTableWrapper.TableWrapperPhasers;
import eu.solven.adhoc.table.transcoder.MapTableAliaser;

public class TestCompositeCubesTableWrapper extends ARawDagTest implements IAdhocTestConstants {

	// Not used
	@Override
	public ITableWrapper makeTable() {
		return InMemoryTable.builder().build();
	}

	private CubeWrapper wrapInCube(IMeasureForest forest, ITableWrapper table) {
		return CubeWrapper.builder()
				.name(table.getName() + ".cube")
				.engine(engine())
				.forest(forest)
				.table(table)
				.build();
	}

	private CubeWrapper makeComposite(CompositeCubesTableWrapper compositeTable, IMeasureForest forest) {
		return CubeWrapper.builder().name("composite").table(compositeTable).forest(forest).engine(engine()).build();
	}

	// `k1` is both an underlyingCube measure, and an explicit cube measure.
	@Test
	public void testAddUnderlyingMeasures_sameMeasureNameInUnderlyingAndInComposite() {
		Aggregator k3Max = Aggregator.builder().name("k3").aggregationKey(MaxAggregation.KEY).build();

		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k2Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k3Max);
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForest withoutUnderlyings = UnsafeMeasureForest.builder().name("composite").build();

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

			CubeWrapper compositeCube = makeComposite(compositeCubesTable, withUnderlyings);

			ITabularView view = compositeCube.execute(CubeQuery.builder().measure(k1Max).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(view);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1Max.getName(), 0L + Math.max(123 + 234, 345 + 456)))
					.hasSize(1);
		}
	}

	@Test
	public void testFilterUnderlyingCube() {
		CompositeCubesTableWrapper composite = CompositeCubesTableWrapper.builder().build();

		ICubeWrapper subCube = Mockito.mock(ICubeWrapper.class);

		Assertions.assertThat(composite.filterForColumns(subCube, ISliceFilter.MATCH_ALL, Set.of()::contains))
				.isEqualTo(ISliceFilter.MATCH_ALL);
		Assertions.assertThat(composite.filterForColumns(subCube, ISliceFilter.MATCH_NONE, Set.of()::contains))
				.isEqualTo(ISliceFilter.MATCH_NONE);

		// and: all columns are known
		Assertions
				.assertThat(composite.filterForColumns(subCube,
						AndFilter.and(ColumnFilter.matchLike("c1", "a%"), ColumnFilter.matchLike("c2", "b%")),
						Set.of("c1", "c2")::contains))
				.isEqualTo(AndFilter.and(ColumnFilter.matchLike("c1", "a%"), ColumnFilter.matchLike("c2", "b%")));

		// and: some columns are unknown
		Assertions.assertThat(composite.filterForColumns(subCube,
				AndFilter.and(ColumnFilter.matchLike("c1", "a%"), ColumnFilter.matchLike("c2", "b%")),
				Set.of("c1")::contains)).isEqualTo(ISliceFilter.MATCH_NONE);

		// or: all columns are known
		Assertions
				.assertThat(composite.filterForColumns(subCube,
						FilterBuilder.or(ColumnFilter.matchLike("c1", "a%"), ColumnFilter.matchLike("c2", "b%"))
								.optimize(),
						Set.of("c1", "c2")::contains))
				.isEqualTo(FilterBuilder.or(ColumnFilter.matchLike("c1", "a%"), ColumnFilter.matchLike("c2", "b%"))
						.combine());

		// or: some columns are unknown
		Assertions.assertThat(composite.filterForColumns(subCube,
				FilterBuilder.or(ColumnFilter.matchLike("c1", "a%"), ColumnFilter.matchLike("c2", "b%")).optimize(),
				Set.of("c1")::contains)).isEqualTo(ColumnFilter.matchLike("c1", "a%"));

		// Or.Not: all columns are known
		Assertions
				.assertThat(composite.filterForColumns(subCube,
						OrFilter.builder()
								.or(ColumnFilter.matchLike("c1", "a%").negate())
								.or(ColumnFilter.matchLike("c2", "b%").negate())
								.build(),
						Set.of("c1", "c2")::contains))
				// The expression is optimized, but still equivalent to the original
				.isEqualTo(FilterBuilder
						.or(ColumnFilter.matchLike("c1", "a%").negate(), ColumnFilter.matchLike("c2", "b%").negate())
						.combine());

		// Or.Not: some columns are unknown
		Assertions.assertThat(composite.filterForColumns(subCube,
				OrFilter.builder()
						.or(ColumnFilter.matchLike("c1", "a%").negate())
						.or(ColumnFilter.matchLike("c2", "b%").negate())
						.build(),
				Set.of("c1")::contains)).isEqualTo(ColumnFilter.matchLike("c1", "a%").negate());
	}

	@Test
	public void testSubQuery_SameUnderlying() {
		CompositeCubesTableWrapper composite = CompositeCubesTableWrapper.builder().build();

		// The subCube has a measure named `k1`
		ICubeWrapper subCube = Mockito.mock(ICubeWrapper.class);
		Mockito.when(subCube.getNameToMeasure()).thenReturn(Map.of(k1Sum.getName(), k1Sum));

		// Request the min and the max of the same measure cross cubes
		TableQueryV2 compositeQuery = TableQueryV2.builder()
				.aggregator(FilteredAggregator.builder()
						.aggregator(k1Sum.toBuilder().name("min").aggregationKey(MinAggregation.KEY).build())
						.build())
				.aggregator(FilteredAggregator.builder()
						.aggregator(k1Sum.toBuilder().name("max").aggregationKey(MaxAggregation.KEY).build())
						.build())
				.build();

		CompatibleMeasures compatibleMeasures =
				composite.computeSubMeasures(compositeQuery, subCube, Set.of()::contains);
		Assertions.assertThat(compatibleMeasures.getPredefined())
				.contains(IMeasure.alias("min", k1Sum.getName()))
				.contains(IMeasure.alias("max", k1Sum.getName()))
				.hasSize(2);
		Assertions.assertThat(compatibleMeasures.getDefined()).isEmpty();
	}

	// Test ensuring the computation of the filter for given subCube is also applied on a per-measure filter
	@Test
	public void testSubQuery_FilteredMeasure() {
		CompositeCubesTableWrapper composite = CompositeCubesTableWrapper.builder().build();

		// The subCube has a measure named `k1`
		ICubeWrapper subCube = Mockito.mock(ICubeWrapper.class);
		Mockito.when(subCube.getNameToMeasure()).thenReturn(Map.of(k1Sum.getName(), k1Sum));

		Set<String> subColumns = Set.of("c1");

		// Request the min and the max of the same measure cross cubes
		TableQueryV2 compositeQuery = TableQueryV2.builder()
				.aggregator(FilteredAggregator.builder()
						.aggregator(k1Sum.toBuilder().name("max_c1").aggregationKey(MaxAggregation.KEY).build())
						.filter(ColumnFilter.matchLike("c1", "a%"))
						.build())
				.aggregator(FilteredAggregator.builder()
						.aggregator(k1Sum.toBuilder().name("max_c2").aggregationKey(MaxAggregation.KEY).build())
						.filter(ColumnFilter.matchLike("c2", "a%"))
						.build())
				.build();

		CompatibleMeasures compatibleMeasures =
				composite.computeSubMeasures(compositeQuery, subCube, subColumns::contains);
		Assertions.assertThat(compatibleMeasures.getPredefined())
				.contains(Filtrator.builder()
						.name("max_c1")
						.underlying(k1Sum.getName())
						.filter(ColumnFilter.matchLike("c1", "a%"))
						.build())
				.contains(Filtrator.builder()
						.name("max_c2")
						.underlying(k1Sum.getName())
						// .filter(ISliceFilter.MATCH_ALL)
						.filter(ISliceFilter.MATCH_NONE)
						.build())
				.hasSize(2);
		Assertions.assertThat(compatibleMeasures.getDefined()).isEmpty();
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
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(countAsterisk);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(countAsterisk);
			cube2 = wrapInCube(measureBag, table2);
		}

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		CubeWrapper cube = makeComposite(compositeCubesTable, forest);
		ITabularView view = cube.execute(CubeQuery.builder()
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
	public void testFilterOnMissingColumn_partiallyKnown() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForest withoutUnderlyings = UnsafeMeasureForest.builder().name("composite").build();
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

			CubeWrapper compositeCube = makeComposite(compositeCubesTable, withoutUnderlyings);

			// groupBy:a
			String missingColumnCoordinate = tableName2 + ".cube";
			{
				ITabularView view =
						compositeCube.execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("a").build());
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
						CubeQuery.builder().measure(k1Sum.getName()).andFilter("a", "unknownCoordinate").build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(mapBased.getCoordinatesToValues()).isEmpty();
			}

			// filter unknownCoordinate groupBy:a
			{
				ITabularView view = compositeCube.execute(CubeQuery.builder()
						.measure(k1Sum.getName())
						.andFilter("a", "unknownCoordinate")
						.groupByAlso("a")
						.build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(mapBased.getCoordinatesToValues()).isEmpty();
			}

			// filter complex groupBy:a
			{
				ITabularView view = compositeCube.execute(CubeQuery.builder()
						.measure(k1Sum.getName())
						.andFilter(ColumnFilter.matchLike("a", "az%"))
						.groupByAlso("a")
						.build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(mapBased.getCoordinatesToValues()).isEmpty();
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
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			cube2 = wrapInCube(measureBag, table2);
		}

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		Assertions.assertThat(compositeCubesTable.getColumnTypes()).containsKey("~CompositeSlicer");
	}

	@Test
	public void testCubeSlicer() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForest withoutUnderlyings = UnsafeMeasureForest.builder().name("composite").build();
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

			CubeWrapper compositeCube = makeComposite(compositeCubesTable, withoutUnderlyings);

			// groupBy:cubeSlicer
			String m = k1Sum.getName();
			{
				ITabularView view =
						compositeCube.execute(CubeQuery.builder().measure(m).groupByAlso("cubeSlicer").build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(mapBased.getCoordinatesToValues())
						.containsEntry(Map.of("cubeSlicer", tableName1 + ".cube"), Map.of(m, 0L + 123 + 234))
						.containsEntry(Map.of("cubeSlicer", tableName2 + ".cube"), Map.of(m, 0L + 345 + 456))
						.hasSize(2);
			}

			// groupBy:cubeSlicer&a
			{
				ITabularView view =
						compositeCube.execute(CubeQuery.builder().measure(m).groupByAlso("cubeSlicer", "a").build());
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

	@Test
	public void testUnknownColumn() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			cube1 = wrapInCube(measureBag, table1);
		}

		UnsafeMeasureForest withoutUnderlyings = UnsafeMeasureForest.builder().name("composite").build();
		withoutUnderlyings.addMeasure(k1Sum);

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).optCubeSlicer(Optional.of("cubeSlicer")).build();

		{
			table1.add(Map.of("k1", 123, "a", "a1"));
			table1.add(Map.of("k1", 234, "a", "a2"));

			Assertions.assertThat(compositeCubesTable.getColumnTypes()).containsKey("cubeSlicer");

			CubeWrapper compositeCube = makeComposite(compositeCubesTable, withoutUnderlyings);

			// groupBy
			String m = k1Sum.getName();
			Assertions.assertThatThrownBy(
					() -> compositeCube.execute(CubeQuery.builder().measure(m).groupByAlso("unknownColumn").build()))
					.isInstanceOf(IllegalArgumentException.class);

			// filter
			Assertions
					.assertThatThrownBy(() -> compositeCube
							.execute(CubeQuery.builder().measure(m).andFilter("unknownColumn", "anyValue").build()))
					.isInstanceOf(IllegalArgumentException.class);
		}
	}

	/**
	 * In this test, we want to test having a Composite Combinator, relying on SubCube Combinators
	 */
	@Test
	public void testWithCombinators() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(Combinator.builder()
					.name("table1_k_minus2")
					.underlying(k1Sum.getName())
					.combinationKey(EvaluatedExpressionCombination.KEY)
					.combinationOptions(Map.of(EvaluatedExpressionCombination.K_EXPRESSION,
							"IF(underlyings[0] == null, null, underlyings[0] - 2)"))
					.build());
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(Combinator.builder()
					.name("table2_k_minus3")
					.underlying(k1Sum.getName())
					.combinationKey(EvaluatedExpressionCombination.KEY)
					.combinationOptions(Map.of(EvaluatedExpressionCombination.K_EXPRESSION,
							"IF(underlyings[0] == null, null, underlyings[0] - 3)"))
					.build());
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForest withoutUnderlyings = UnsafeMeasureForest.builder().name("composite").build();
		withoutUnderlyings.addMeasure(IMeasure.sum("compositeSum", "composite_power2", "composite_power3"));
		withoutUnderlyings.addMeasure(Combinator.builder()
				.name("composite_power2")
				.underlying("table1_k_minus2")
				.combinationKey(EvaluatedExpressionCombination.KEY)
				.combinationOptions(Map.of(EvaluatedExpressionCombination.K_EXPRESSION,
						"IF(underlyings[0] == null, null, underlyings[0] * underlyings[0])"))
				.build());
		withoutUnderlyings.addMeasure(Combinator.builder()
				.name("composite_power3")
				.underlying("table2_k_minus3")
				.combinationKey(EvaluatedExpressionCombination.KEY)
				.combinationOptions(Map.of(EvaluatedExpressionCombination.K_EXPRESSION,
						"IF(underlyings[0] == null, null, underlyings[0] * underlyings[0] * underlyings[0])"))
				.build());

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		IMeasureForest withUnderlyings = compositeCubesTable.injectUnderlyingMeasures(withoutUnderlyings);

		{
			List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

			table1.add(Map.of("k1", 123, "a", "a1"));
			table1.add(Map.of("k1", 234, "a", "a2"));

			table2.add(Map.of("k1", 345));
			table2.add(Map.of("k1", 456));

			CubeWrapper compositeCube = makeComposite(compositeCubesTable, withUnderlyings);

			// groupBy:cubeSlicer
			String m = "compositeSum";
			{
				ITabularView view = compositeCube.execute(CubeQuery.builder().measure(m).explain(true).build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(view);

				Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
						.isEqualToNormalizingNewlines(
								"""
										/-- time=6ms for openingStream
										|/- time=8ms for mergingAggregates
										|/- time=9ms sizes=[1] for sortingColumns
										\\------ time=35ms for tableQuery on SELECT k1:SUM(k1) WHERE matchAll GROUP BY ()
										/-- #0 s=someTableName1 id=00000000-0000-0000-0000-000000000001 (parentId=00000000-0000-0000-0000-000000000000)
										|      No cost info
										\\-- #1 m=table1_k_minus2(Combinator[EXPRESSION]) filter=matchAll groupBy=grandTotal
										    |  size=1 duration=10ms
										    \\-- #2 m=k1(SUM) filter=matchAll groupBy=grandTotal
										        \\  size=1 duration=24ms
										Executed status=OK duration=49ms on table=someTableName1 forest=someTableName1-filtered query=AdhocSubQuery(subQuery=CubeQuery(filter=matchAll, groupBy=grandTotal, measures=[ReferencedMeasure(ref=table1_k_minus2)], customMarker=null, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED]), parentQueryId=AdhocQueryId(queryIndex=0, queryId=00000000-0000-0000-0000-000000000000, parentQueryId=null, queryHash=3de24a35, cube=composite))
										/-- time=13ms for openingStream
										|/- time=15ms for mergingAggregates
										|/- time=16ms sizes=[1] for sortingColumns
										\\------ time=70ms for tableQuery on SELECT k1:SUM(k1) WHERE matchAll GROUP BY ()
										/-- #0 s=someTableName2 id=00000000-0000-0000-0000-000000000002 (parentId=00000000-0000-0000-0000-000000000000)
										|      No cost info
										\\-- #1 m=table2_k_minus3(Combinator[EXPRESSION]) filter=matchAll groupBy=grandTotal
										    |  size=1 duration=17ms
										    \\-- #2 m=k1(SUM) filter=matchAll groupBy=grandTotal
										        \\  size=1 duration=45ms
										Executed status=OK duration=98ms on table=someTableName2 forest=someTableName2-filtered query=AdhocSubQuery(subQuery=CubeQuery(filter=matchAll, groupBy=grandTotal, measures=[ReferencedMeasure(ref=table2_k_minus3)], customMarker=null, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED]), parentQueryId=AdhocQueryId(queryIndex=0, queryId=00000000-0000-0000-0000-000000000000, parentQueryId=null, queryHash=3de24a35, cube=composite))
										/-- time=150ms for openingStream
										|/- time=19ms for mergingAggregates
										|/- time=20ms sizes=[1, 1] for sortingColumns
										\\------ time=209ms for tableQuery on SELECT table1_k_minus2:SUM(table1_k_minus2), table2_k_minus3:SUM(table2_k_minus3) WHERE matchAll GROUP BY ()
										/-- #0 s=composite id=00000000-0000-0000-0000-000000000000
										|      No cost info
										\\-- #1 m=compositeSum(Combinator[SUM]) filter=matchAll groupBy=grandTotal
										    |  size=1 duration=23ms
										    |\\- #2 m=composite_power2(Combinator[EXPRESSION]) filter=matchAll groupBy=grandTotal
										    |   |  size=1 duration=21ms
										    |   \\-- #3 m=table1_k_minus2(SUM) filter=matchAll groupBy=grandTotal
										    |       \\  size=1 duration=57ms
										    \\-- #4 m=composite_power3(Combinator[EXPRESSION]) filter=matchAll groupBy=grandTotal
										        |  size=1 duration=22ms
										        \\-- #5 m=table2_k_minus3(SUM) filter=matchAll groupBy=grandTotal
										            \\  size=1 duration=57ms
										Executed status=OK duration=276ms on table=composite forest=composite-filtered query=CubeQuery(filter=matchAll, groupBy=grandTotal, measures=[ReferencedMeasure(ref=compositeSum)], customMarker=null, options=[EXPLAIN])""")
						.hasLineCount(39);

				Assertions.assertThat(mapBased.getCoordinatesToValues())
						.containsEntry(Map.of(),
								Map.of(m, 0L + (long) Math.pow(123 + 234 - 2, 2) + (long) Math.pow(345 + 456 - 3, 3)))
						.hasSize(1);
			}
		}
	}

	@Disabled("TODO WIP over EXCEPTIONS_AS_MEASURE_VALUE")
	@Test
	public void testExceptionsAsMeasure() {
		Aggregator k3Max = Aggregator.builder().name("k3").aggregationKey(MaxAggregation.KEY).build();

		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		FailingTableWrapper table2 = FailingTableWrapper.builder().name(tableName2).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k2Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k3Max);
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForest withoutUnderlyings = UnsafeMeasureForest.builder().name("composite").build();

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		IMeasureForest withUnderlyings = compositeCubesTable.injectUnderlyingMeasures(withoutUnderlyings);

		// Test an actual query result
		{
			table1.add(Map.of("k1", 123));
			table1.add(Map.of("k1", 234));

			CubeWrapper compositeCube = makeComposite(compositeCubesTable, withUnderlyings);

			Assertions.assertThatThrownBy(() -> compositeCube.execute(CubeQuery.builder().measure("k1").build()))
					.isInstanceOf(IllegalStateException.class)
					.hasRootCauseMessage("Simulating some exception");

			ITabularView view = compositeCube.execute(
					CubeQuery.builder().measure("k1").option(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(view);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of("k1", 0L + Math.max(123 + 234, 345 + 456)))
					.hasSize(1);
		}
	}

	// This forces a FilteredAggregator with a not trivial aggregator to reach the CompositeTableWrapper. This is a
	// matter as ICubeQuery has no such `FILTER` per measure. We typically build on-the-fly a Filtrator measure.
	@Test
	public void testQueryUnderlyingWithDifferentFilters() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k2Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForest withoutUnderlyings = UnsafeMeasureForest.builder().name("composite").build();
		withoutUnderlyings.addMeasure(filterK1onA1);

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		IMeasureForest withUnderlyings = compositeCubesTable.injectUnderlyingMeasures(withoutUnderlyings);

		// Test an actual query result
		{
			// table1
			table1.add(Map.of("k1", 123, "a", "a1"));
			table1.add(Map.of("k1", 234, "a", "a2"));
			// table2
			table2.add(Map.of("k1", 345, "a", "a1"));

			CubeWrapper compositeCube = makeComposite(compositeCubesTable, withUnderlyings);

			ITabularView view =
					compositeCube.execute(CubeQuery.builder().measure(k1Sum.getName(), filterK1onA1.getName()).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(view);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(),
							Map.of(k1Sum.getName(), 0L + 123 + 234 + 345, filterK1onA1.getName(), 0L + 123 + 345))
					.hasSize(1);
		}
	}

	@Test
	public void testGetColumns_alias() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();

		ICubeWrapper cube1;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		ICubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			cube2 = wrapInCube(measureBag, table2);

			cube2 = CubeWrapperEditor.edit(cube2)
					.aliaser(MapTableAliaser.builder().aliasToOriginal("a", "_a").build())
					.build();
		}

		UnsafeMeasureForest withoutUnderlyings = UnsafeMeasureForest.builder().name("composite").build();
		withoutUnderlyings.addMeasure(k1Sum);

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		{
			table1.add(Map.of("k1", 123, "a", "a1"));
			table1.add(Map.of("k1", 234, "a", "a2"));

			table2.add(Map.of("k1", 345, "_a", "a1"));
			table2.add(Map.of("k1", 456, "_a", "a2"));

			// Consider a column in one table but not the other
			Assertions.assertThat(table1.getColumnTypes()).containsKey("a");
			Assertions.assertThat(table2.getColumnTypes()).doesNotContainKey("a");

			CubeWrapper compositeCube = makeComposite(compositeCubesTable, withoutUnderlyings);

			Assertions.assertThat(compositeCube.getColumnsAsMap())
					.containsEntry("a",
							ColumnMetadata.builder().name("a").tag("composite-full").type(String.class).build())
					.containsEntry("_a",
							ColumnMetadata.builder()
									.name("_a")
									.tag("composite-partial")
									.tag("composite-unknown:" + cube1.getName())
									.tag("composite-known:" + cube2.getName())
									.type(String.class)
									.build())
					.containsEntry("k1",
							ColumnMetadata.builder().name("k1").tag("composite-full").type(Integer.class).build())
					.containsEntry("~CompositeSlicer",
							ColumnMetadata.builder()
									.name("~CompositeSlicer")
									.tag("meta")
									.tag("composite-full")
									.type(String.class)
									.build())
					.hasSize(4);

			ITabularView view =
					compositeCube.execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("a").build());
			Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
					.hasSize(2)
					.containsEntry(Map.of("a", "a1"), Map.of(k1Sum.getName(), 0L + 123 + 345))
					.containsEntry(Map.of("a", "a2"), Map.of(k1Sum.getName(), 0L + 234 + 456));
		}
	}

	@Test
	public void testCompositeSlicerIsLastByDefault() {
		CompositeCubesTableWrapper compositeCubesTable = CompositeCubesTableWrapper.builder().build();

		Assertions.assertThat(compositeCubesTable.optCubeSlicer.get())
				.usingComparator(Comparator.naturalOrder())
				.isGreaterThan("a")
				.isGreaterThan("z")
				.isGreaterThan("A")
				.isGreaterThan("Z")
				.isGreaterThan("0")
				.isGreaterThan("9");

	}
}
