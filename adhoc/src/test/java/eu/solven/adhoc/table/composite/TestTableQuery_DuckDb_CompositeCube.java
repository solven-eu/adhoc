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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.UnsafeMeasureForest;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.ADuckDbJooqTest;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;

public class TestTableQuery_DuckDb_CompositeCube extends ADuckDbJooqTest implements IAdhocTestConstants {

	Aggregator k3Sum = Aggregator.sum("k3");

	@Override
	public ITableWrapper makeTable() {
		throw new UnsupportedOperationException("This test has multiple main tables");
	}

	String tableName1 = "someTableName1";
	JooqTableWrapper table1 = new JooqTableWrapper(tableName1,
			JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName1).build());

	String tableName2 = "someTableName2";
	JooqTableWrapper table2 = new JooqTableWrapper(tableName2,
			JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName2).build());

	private CubeWrapper wrapInCube(IMeasureForest forest, JooqTableWrapper table) {
		return CubeWrapper.builder()
				.name(table.getName() + ".cube")
				.engine(engine())
				.forest(forest)
				.table(table)
				.build();
	}

	private CubeWrapper makeAndFeedCompositeCube() {
		return makeAndFeedCompositeCube(forest -> {
			// Stick to default behavior
		}, forest -> {
			// Stick to default behavior
		}, forest -> {
			// Stick to default behavior
		});
	}

	private CubeWrapper makeAndFeedCompositeCube(Consumer<UnsafeMeasureForest> onForest1,
			Consumer<UnsafeMeasureForest> onForest2,
			Consumer<UnsafeMeasureForest> onCompositeForestWithoutSubs) {
		CubeWrapper cube1;
		{
			dsl.createTableIfNotExists(tableName1)
					// k1 is in both cubes
					.column("k1", SQLDataType.INTEGER)
					// k2 is only in cube1
					.column("k2", SQLDataType.INTEGER)
					// k4 is in both cubes, but there is no measure with the column name
					.column("k4", SQLDataType.DOUBLE)
					.column("a", SQLDataType.VARCHAR)
					.column("b", SQLDataType.VARCHAR)
					.execute();
			dsl.insertInto(DSL.table(
					tableName1), DSL.field("k1"), DSL.field("k2"), DSL.field("k4"), DSL.field("a"), DSL.field("b"))
					.values(123, 234, 567, "a1", "b1")
					.execute();
			dsl.insertInto(DSL.table(
					tableName1), DSL.field("k1"), DSL.field("k2"), DSL.field("k4"), DSL.field("a"), DSL.field("b"))
					.values(345, 456, 678, "a2", "b2")
					.execute();
			UnsafeMeasureForest forest = UnsafeMeasureForest.builder().name(tableName1).build();
			forest.addMeasure(k1Sum);
			forest.addMeasure(k2Sum);
			forest.addMeasure(Aggregator.countAsterisk());
			onForest1.accept(forest);
			cube1 = wrapInCube(forest, table1);
		}
		CubeWrapper cube2;
		{
			dsl.createTableIfNotExists(tableName2)
					.column("k1", SQLDataType.INTEGER)
					.column("k3", SQLDataType.INTEGER)
					// k4 is in both cubes, but there is no measure with the column name
					.column("k4", SQLDataType.INTEGER)
					.column("a", SQLDataType.VARCHAR)
					.column("c", SQLDataType.VARCHAR)
					.execute();
			dsl.insertInto(DSL.table(
					tableName2), DSL.field("k1"), DSL.field("k3"), DSL.field("k4"), DSL.field("a"), DSL.field("c"))
					.values(1234, 2345, 3456, "a1", "c1")
					.execute();
			UnsafeMeasureForest forest = UnsafeMeasureForest.builder().name(tableName2).build();
			forest.addMeasure(k1Sum);
			forest.addMeasure(k3Sum);
			forest.addMeasure(Aggregator.countAsterisk());
			onForest2.accept(forest);
			cube2 = wrapInCube(forest, table2);
		}

		UnsafeMeasureForest forestWithoutSubs = UnsafeMeasureForest.builder().name("composite").build();
		forestWithoutSubs.addMeasure(k1PlusK2AsExpr);
		onCompositeForestWithoutSubs.accept(forestWithoutSubs);

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		IMeasureForest measureBagWithUnderlyings = compositeCubesTable.injectUnderlyingMeasures(forestWithoutSubs);

		CubeWrapper cube3 = CubeWrapper.builder()
				.engine(engine())
				.forest(measureBagWithUnderlyings)
				.table(compositeCubesTable)
				.build();
		return cube3;
	}

	@Test
	public void testGetColumns() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		Assertions.assertThat(cube3.getColumnTypes())
				.containsEntry("a", String.class)
				.containsEntry("b", String.class)
				.containsEntry("c", String.class)
				.containsEntry(k1Sum.getColumnName(), Integer.class)
				.containsEntry(k2Sum.getColumnName(), Integer.class)
				.containsEntry(k3Sum.getColumnName(), Integer.class)
				.containsEntry("k4", Number.class)
				.containsEntry("cubeSlicer", String.class)
				.hasSize(8);

		Assertions.assertThat(cube3.getColumns()).anySatisfy(c -> {
			Assertions.assertThat(c.getName()).isEqualTo("cubeSlicer");
			Assertions.assertThat(c.getTags()).contains("adhoc");
		}).hasSize(8);

		Assertions.assertThat(cube3.getNameToMeasure().keySet())
				.contains(k1Sum.getColumnName())
				.contains(k1PlusK2AsExpr.getName())
				.contains(k2Sum.getColumnName())
				.contains(k3Sum.getColumnName())
				.contains(Aggregator.countAsterisk().getName())
				.hasSize(5);
	}

	@Test
	public void testQueryCube1() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		// `k2` does not exists in cube2
		{
			ITabularView result = cube3.execute(CubeQuery.builder().measure(k2Sum.getName()).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k2Sum.getName(), 0L + 234 + 456))
					.hasSize(1);
		}

		// `k2` does not exists in cube2 and `a` is in both cubes
		{
			ITabularView result = cube3.execute(CubeQuery.builder().measure(k2Sum.getName()).groupByAlso("a").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("a", "a1"), Map.of(k2Sum.getName(), 0L + 234))
					.containsEntry(Map.of("a", "a2"), Map.of(k2Sum.getName(), 0L + 456))
					.hasSize(2);
		}

		// `k2` does not exists in cube2 and `b` does not exists in cube2
		{
			ITabularView result = cube3.execute(CubeQuery.builder().measure(k2Sum.getName()).groupByAlso("b").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("b", "b1"), Map.of(k2Sum.getName(), 0L + 234))
					.containsEntry(Map.of("b", "b2"), Map.of(k2Sum.getName(), 0L + 456))
					.hasSize(2);
		}
	}

	@Test
	public void testQueryCube1Plus2() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		// grandTotal
		{
			ITabularView result = cube3.execute(CubeQuery.builder().measure(k1PlusK2AsExpr.getName()).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 345 + 456 + 1234))
					.hasSize(1);
		}

		// `a` is in both cubes
		{
			ITabularView result =
					cube3.execute(CubeQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("a").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("a", "a1"), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 1234))
					.containsEntry(Map.of("a", "a2"), Map.of(k1PlusK2AsExpr.getName(), 0L + 345 + 456))
					.hasSize(2);
		}

		// `b` does not exists in cube2
		{
			ITabularView result =
					cube3.execute(CubeQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("b").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("b", "b1"), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234))
					.containsEntry(Map.of("b", "b2"), Map.of(k1PlusK2AsExpr.getName(), 0L + 345 + 456))
					.containsEntry(Map.of("b", "someTableName2.cube"), Map.of(k1PlusK2AsExpr.getName(), 0L + 1234))
					.hasSize(3);
		}
	}

	@Test
	public void testQueryCube1Plus2_groupByShared() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result =
				cube3.execute(CubeQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("a").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 1234))
				.containsEntry(Map.of("a", "a2"), Map.of(k1PlusK2AsExpr.getName(), 0L + 345 + 456))
				.hasSize(2);
	}

	@Test
	public void testQueryCube1Plus2_groupByUnshared() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result =
				cube3.execute(CubeQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("b").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("b", "b1"), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234))
				.containsEntry(Map.of("b", "b2"), Map.of(k1PlusK2AsExpr.getName(), 0L + 345 + 456))
				.containsEntry(Map.of("b", "someTableName2.cube"), Map.of(k1PlusK2AsExpr.getName(), 0L + 1234))
				.hasSize(3);
	}

	@Test
	public void testQueryCube1Plus2_filterShared() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result =
				cube3.execute(CubeQuery.builder().measure(k1PlusK2AsExpr.getName()).andFilter("a", "a1").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 1234))
				.hasSize(1);
	}

	@Test
	public void testQueryCube1Plus2_filterUnshared() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplainNoPerf(eventBus);

		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(
				CubeQuery.builder().measure(k1PlusK2AsExpr.getName()).andFilter("b", "b1").explain(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 1234))
				.hasSize(1);

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualTo(
						"""
								#0 s=composite id=00000000-0000-0000-0000-000000000000
								\\-- #1 m=k1PlusK2AsExpr(Combinator[EXPRESSION]) filter=b==b1 groupBy=grandTotal
								    |\\- #2 m=k1(SUM) filter=b==b1 groupBy=grandTotal
								    \\-- #3 m=k2(SUM) filter=b==b1 groupBy=grandTotal
								#0 s=someTableName1 id=00000000-0000-0000-0000-000000000001 (parentId=00000000-0000-0000-0000-000000000000)
								|\\- #1 m=k1(SUM) filter=b==b1 groupBy=grandTotal
								\\-- #2 m=k2(SUM) filter=b==b1 groupBy=grandTotal
								#0 s=someTableName2 id=00000000-0000-0000-0000-000000000002 (parentId=00000000-0000-0000-0000-000000000000)
								\\-- #1 m=k1(SUM) filter=matchAll groupBy=grandTotal"""
								.trim());
		Assertions.assertThat(messages).hasSize(9);
	}

	@Test
	public void testQuery_Count() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(CubeQuery.builder().measure(Aggregator.countAsterisk()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(Aggregator.countAsterisk().getName(), 0L + 2 + 1))
				.hasSize(1);
	}

	// Make sure Composite can not only do Linear SUMs
	@Test
	public void testQuery_MinMax() {
		CubeWrapper initialCube3 = makeAndFeedCompositeCube();

		// We add k1Min and k1Max in the composite cube: these measures are not known from the underlying cubes.
		CubeWrapper cube3 = initialCube3.toBuilder()
				.forest(MeasureForest.edit(initialCube3.getForest())
						.measure(Aggregator.builder()
								.name("k1Min")
								// This refer to `k1` as a compositeColumn
								// BEWARE It is ambiguous as `k1` is either `measure=composite.k1`, hence doing the
								// min of the measure `k1` per cube (`cube1.k1` being maybe a max).
								// Or to `column=table1.k1|table2.k1`, hence doing the min of the cube.
								.columnName("k1")
								.aggregationKey(MinAggregation.KEY)
								.build())
						.measure(Aggregator.builder()
								.name("k1Max")
								.columnName("k1")
								.aggregationKey(MaxAggregation.KEY)
								.build())
						.build())
				.build();

		ITabularView result = cube3.execute(CubeQuery.builder().measure("k1Min", "k1Max").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of("k1Min", 0L + 123 + 345, "k1Max", 0L + 1234))
				.hasSize(1);
	}

	// Check aggregation with Carrier. Rank is especially difficult as we need the table to provide the TOP-N, so they
	// are merged in the composite.
	@Test
	public void testQuery_Rank() {
		CubeWrapper initialCube3 = makeAndFeedCompositeCube();

		// We add k1Min and k1Max in the composite cube: these measures are not known from the underlying cubes.
		CubeWrapper cube3 = initialCube3.toBuilder()
				.forest(MeasureForest.edit(initialCube3.getForest())
						.measure(Aggregator.builder()
								.name("k1Rank1")
								.columnName("k1")
								.aggregationKey(RankAggregation.KEY)
								.aggregationOption(RankAggregation.P_RANK, 1)
								.build())
						.measure(Aggregator.builder()
								.name("k1Rank2")
								.columnName("k1")
								.aggregationKey(RankAggregation.KEY)
								.aggregationOption(RankAggregation.P_RANK, 2)
								.build())
						.measure(Aggregator.builder()
								.name("k1Rank3")
								.columnName("k1")
								.aggregationKey(RankAggregation.KEY)
								.aggregationOption(RankAggregation.P_RANK, 3)
								.build())
						.measure(Aggregator.builder()
								.name("k4Rank3")
								.columnName("k4")
								.aggregationKey(RankAggregation.KEY)
								.aggregationOption(RankAggregation.P_RANK, 1)
								.build())
						.build())
				.build();

		ITabularView result = cube3.execute(CubeQuery.builder().measure("k1Rank1", "k1Rank2", "k1Rank3").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of("k1Rank1", 0L + 1234, "k1Rank2", 0L + 123 + 345))
				.hasSize(1);
	}

	@Test
	public void testQuery_Avg() {
		CubeWrapper initialCube3 = makeAndFeedCompositeCube();

		// `k1` is a measure of both cubes: Doing the AVG will compute `k1` per cube and the avg
		{
			CubeWrapper cube3 = initialCube3.toBuilder()
					.forest(MeasureForest.edit(initialCube3.getForest())
							.measure(Aggregator.builder()
									.name("avg")
									.columnName("k1")
									.aggregationKey(AvgAggregation.KEY)
									.build())
							.build())
					.build();

			ITabularView result = cube3.execute(CubeQuery.builder().measure("avg").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					// Divide by 2 cubes
					.containsEntry(Map.of(), Map.of("avg", (0L + 123 + 345 + 1234) / 2))
					.hasSize(1);
		}

		// `k2` is a measure of only cube1: Doing the AVG will compute `k2` per cube and the avg
		{
			CubeWrapper cube3 = initialCube3.toBuilder()
					.forest(MeasureForest.edit(initialCube3.getForest())
							.measure(Aggregator.builder()
									.name("avg")
									.columnName("k2")
									.aggregationKey(AvgAggregation.KEY)
									.build())
							.build())
					.build();

			ITabularView result = cube3.execute(CubeQuery.builder().measure("avg").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					// Only a single cube provides given measure, and the other does not have a k2 column
					.containsEntry(Map.of(), Map.of("avg", (0L + 234 + 456) / 1))
					.hasSize(1);
		}

		// `k4` is a column of only cube1: Doing the AVG will propagate the AVG definition of `k4` in cube1
		{
			CubeWrapper cube3 = initialCube3.toBuilder()
					.forest(MeasureForest.edit(initialCube3.getForest())
							.measure(Aggregator.builder()
									.name("avg")
									.columnName("k4")
									.aggregationKey(AvgAggregation.KEY)
									.build())
							.build())
					.build();

			ITabularView result = cube3.execute(CubeQuery.builder().measure("avg").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					// BEWARE THIS IS A WRONG RESULT
					// `k4.avg` is evaluated by DuckDB, and it is NOT re-wrapped in a Carrier, which is improper
					// behavior.
					.containsEntry(Map.of(), Map.of("avg", (0D + (0D + 567 + 678) / 2 + 3456) / 2))
					.hasSize(1);
		}
	}

	// BEWARE This is a faulty behavior. We query an average from DuckDB, while we need to built a carrier, as it is
	// necessary by the composite.
	@Test
	public void testQuery_AvgAggregator() {
		String mAvg = "k1.avg";

		CubeWrapper compositeCube = makeAndFeedCompositeCube(forest -> {
			forest.addMeasure(
					Aggregator.builder().name(mAvg).columnName("k1").aggregationKey(AvgAggregation.KEY).build());
		}, forest -> {
			forest.addMeasure(
					Aggregator.builder().name(mAvg).columnName("k1").aggregationKey(AvgAggregation.KEY).build());
		}, forest -> {
			forest.addMeasure(Aggregator.builder().name(mAvg).aggregationKey(AvgAggregation.KEY).build());
		});

		ITabularView result = compositeCube.execute(CubeQuery.builder().measure(mAvg).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(mAvg, (0D + (123 + 345) / 2 + 1234) / 2))
				.hasSize(1);
	}

	@Test
	public void testQueryNoMeasures_common() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(CubeQuery.builder().groupByAlso("a").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of())
				.containsEntry(Map.of("a", "a2"), Map.of())
				.hasSize(2);
	}

	@Test
	public void testQueryNoMeasures_onlyOne() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(CubeQuery.builder().groupByAlso("b", "c").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("b", "b1", "c", "someTableName1.cube"), Map.of())
				.containsEntry(Map.of("b", "b2", "c", "someTableName1.cube"), Map.of())
				.containsEntry(Map.of("b", "someTableName2.cube", "c", "c1"), Map.of())
				.hasSize(3);
	}

	@Test
	public void testLogPerfs() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

		CubeWrapper cube3 = makeAndFeedCompositeCube();

		cube3.execute(CubeQuery.builder()
				.measure("k1", "k2", "k3")
				.customMarker(Optional.of("JPY"))
				// TODO `letter` is pointless in this class as it is unknown by all cubes
				.groupByAlso("letter")
				.andFilter("color", "red")
				.explain(true)
				.build());

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualToNormalizingNewlines(
						"""
								time=4ms for openingStream on TableQueryV2(filter=matchAll, groupBy=grandTotal, aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k2, tags=[], columnName=k2, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								time=6ms for mergeTableAggregates on TableQueryV2(filter=matchAll, groupBy=grandTotal, aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k2, tags=[], columnName=k2, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								time=7ms sizes=[1, 1] for toSortedColumns on TableQueryV2(filter=matchAll, groupBy=grandTotal, aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k2, tags=[], columnName=k2, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								#0 s=someTableName1 id=00000000-0000-0000-0000-000000000001 (parentId=00000000-0000-0000-0000-000000000000)
								|  No cost info
								|\\- #1 m=k1(SUM) filter=matchAll groupBy=grandTotal customMarker=JPY
								|   \\  size=1 duration=18ms
								\\-- #2 m=k2(SUM) filter=matchAll groupBy=grandTotal customMarker=JPY
								    \\  size=1 duration=18ms
								Executed status=OK duration=25ms on table=someTableName1 forest=someTableName1-filtered query=AdhocSubQuery(subQuery=CubeQuery(filter=matchAll, groupBy=grandTotal, measures=[ReferencedMeasure(ref=k1), ReferencedMeasure(ref=k2)], customMarker=JPY, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED]), parentQueryId=AdhocQueryId(queryIndex=0, queryId=00000000-0000-0000-0000-000000000000, parentQueryId=null, queryHash=b1f73b13, cube=composite))
								time=9ms for openingStream on TableQueryV2(filter=matchAll, groupBy=grandTotal, aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k3, tags=[], columnName=k3, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								time=11ms for mergeTableAggregates on TableQueryV2(filter=matchAll, groupBy=grandTotal, aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k3, tags=[], columnName=k3, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								time=12ms sizes=[1, 1] for toSortedColumns on TableQueryV2(filter=matchAll, groupBy=grandTotal, aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k3, tags=[], columnName=k3, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								#0 s=someTableName2 id=00000000-0000-0000-0000-000000000002 (parentId=00000000-0000-0000-0000-000000000000)
								|  No cost info
								|\\- #1 m=k1(SUM) filter=matchAll groupBy=grandTotal customMarker=JPY
								|   \\  size=1 duration=33ms
								\\-- #2 m=k3(SUM) filter=matchAll groupBy=grandTotal customMarker=JPY
								    \\  size=1 duration=33ms
								Executed status=OK duration=50ms on table=someTableName2 forest=someTableName2-filtered query=AdhocSubQuery(subQuery=CubeQuery(filter=matchAll, groupBy=grandTotal, measures=[ReferencedMeasure(ref=k3), ReferencedMeasure(ref=k1)], customMarker=JPY, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED]), parentQueryId=AdhocQueryId(queryIndex=0, queryId=00000000-0000-0000-0000-000000000000, parentQueryId=null, queryHash=b1f73b13, cube=composite))
								time=77ms for openingStream on TableQueryV2(filter=color==red, groupBy=(letter), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k2, tags=[], columnName=k2, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k3, tags=[], columnName=k3, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN])
								time=14ms for mergeTableAggregates on TableQueryV2(filter=color==red, groupBy=(letter), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k2, tags=[], columnName=k2, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k3, tags=[], columnName=k3, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN])
								time=15ms sizes=[2, 1, 1] for toSortedColumns on TableQueryV2(filter=color==red, groupBy=(letter), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k2, tags=[], columnName=k2, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k3, tags=[], columnName=k3, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN])
								#0 s=composite id=00000000-0000-0000-0000-000000000000
								|  No cost info
								|\\- #1 m=k1(SUM) filter=color==red groupBy=(letter) customMarker=JPY
								|   \\  size=2 duration=42ms
								|\\- #2 m=k2(SUM) filter=color==red groupBy=(letter) customMarker=JPY
								|   \\  size=1 duration=42ms
								\\-- #3 m=k3(SUM) filter=color==red groupBy=(letter) customMarker=JPY
								    \\  size=1 duration=42ms
								Executed status=OK duration=120ms on table=composite forest=composite-filtered query=CubeQuery(filter=color==red, groupBy=(letter), measures=[ReferencedMeasure(ref=k1), ReferencedMeasure(ref=k2), ReferencedMeasure(ref=k3)], customMarker=JPY, options=[EXPLAIN])""");

		Assertions.assertThat(messages).hasSize(22);
	}

	@Test
	public void testLogPerfs_conflictingSubMeasure() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

		CubeWrapper cube3 = makeAndFeedCompositeCube(forest -> {
		}, forest -> {
		}, forest -> {
			// Register k1Max in composite with same name as underlying k1Sum
			forest.addMeasure(k1Sum.toBuilder().aggregationKey(MaxAggregation.KEY).build());
		});

		String mComposite = k1Sum.getName();
		String mSub = k1Sum.getName() + "." + ("someTableName1" + ".cube");
		ITabularView view = cube3.execute(CubeQuery.builder()
				// Request both the composite conflicting and the sub conflicting measures
				.measure(mComposite, mSub)
				.customMarker(Optional.of("JPY"))
				.groupByAlso("a")
				.andFilter("color", "red")
				.explain(true)
				.build());

		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"), Map.of(mComposite, 0L + Math.max(1234, 123), mSub, 0L + 1234 + 123))
				// TODO `mSub` should be meaningless for cube2
				.containsEntry(Map.of("a", "a2"), Map.of(mComposite, 0L + 345, mSub, 0L + 345));

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualToNormalizingNewlines(
						"""
								time=4ms for openingStream on TableQueryV2(filter=matchAll, groupBy=(a), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								time=6ms for mergeTableAggregates on TableQueryV2(filter=matchAll, groupBy=(a), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								time=7ms sizes=[2] for toSortedColumns on TableQueryV2(filter=matchAll, groupBy=(a), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								#0 s=someTableName1 id=00000000-0000-0000-0000-000000000001 (parentId=00000000-0000-0000-0000-000000000000)
								|  No cost info
								|\\- #1 m=k1(SUM) filter=matchAll groupBy=(a) customMarker=JPY
								|   \\  size=2 duration=18ms
								\\-- #2 m=k1.someTableName1.cube(Combinator[COALESCE]) filter=matchAll groupBy=(a) customMarker=JPY
								    |  size=2 duration=8ms
								    \\-- !1
								Executed status=OK duration=33ms on table=someTableName1 forest=someTableName1-filtered query=AdhocSubQuery(subQuery=CubeQuery(filter=matchAll, groupBy=(a), measures=[Combinator(name=k1.someTableName1.cube, tags=[], underlyings=[k1], combinationKey=COALESCE, combinationOptions={}), ReferencedMeasure(ref=k1)], customMarker=JPY, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED]), parentQueryId=AdhocQueryId(queryIndex=0, queryId=00000000-0000-0000-0000-000000000000, parentQueryId=null, queryHash=cf9140ac, cube=composite))
								time=10ms for openingStream on TableQueryV2(filter=matchAll, groupBy=(a), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								time=12ms for mergeTableAggregates on TableQueryV2(filter=matchAll, groupBy=(a), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								time=13ms sizes=[1] for toSortedColumns on TableQueryV2(filter=matchAll, groupBy=(a), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED])
								#0 s=someTableName2 id=00000000-0000-0000-0000-000000000002 (parentId=00000000-0000-0000-0000-000000000000)
								|  No cost info
								|\\- #1 m=k1(SUM) filter=matchAll groupBy=(a) customMarker=JPY
								|   \\  size=1 duration=36ms
								\\-- #2 m=k1.someTableName1.cube(Combinator[COALESCE]) filter=matchAll groupBy=(a) customMarker=JPY
								    |  size=1 duration=14ms
								    \\-- !1
								Executed status=OK duration=69ms on table=someTableName2 forest=someTableName2-filtered query=AdhocSubQuery(subQuery=CubeQuery(filter=matchAll, groupBy=(a), measures=[Combinator(name=k1.someTableName1.cube, tags=[], underlyings=[k1], combinationKey=COALESCE, combinationOptions={}), ReferencedMeasure(ref=k1)], customMarker=JPY, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED]), parentQueryId=AdhocQueryId(queryIndex=0, queryId=00000000-0000-0000-0000-000000000000, parentQueryId=null, queryHash=cf9140ac, cube=composite))
								time=104ms for openingStream on TableQueryV2(filter=color==red, groupBy=(a), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=MAX, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k1.someTableName1.cube, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN])
								time=16ms for mergeTableAggregates on TableQueryV2(filter=color==red, groupBy=(a), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=MAX, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k1.someTableName1.cube, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN])
								time=17ms sizes=[2, 2] for toSortedColumns on TableQueryV2(filter=color==red, groupBy=(a), aggregators=[FilteredAggregator(aggregator=Aggregator(name=k1, tags=[], columnName=k1, aggregationKey=MAX, aggregationOptions={}), filter=matchAll, index=0), FilteredAggregator(aggregator=Aggregator(name=k1.someTableName1.cube, tags=[], columnName=k1, aggregationKey=SUM, aggregationOptions={}), filter=matchAll, index=0)], customMarker=JPY, topClause=noLimit, options=[EXPLAIN])
								#0 s=composite id=00000000-0000-0000-0000-000000000000
								|  No cost info
								|\\- #1 m=k1(MAX) filter=color==red groupBy=(a) customMarker=JPY
								|   \\  size=2 duration=48ms
								\\-- #2 m=k1.someTableName1.cube(SUM) filter=color==red groupBy=(a) customMarker=JPY
								    \\  size=2 duration=48ms
								Executed status=OK duration=153ms on table=composite forest=composite-filtered query=CubeQuery(filter=color==red, groupBy=(a), measures=[ReferencedMeasure(ref=k1), ReferencedMeasure(ref=k1.someTableName1.cube)], customMarker=JPY, options=[EXPLAIN])""");

		Assertions.assertThat(messages).hasSize(23);
	}
}
