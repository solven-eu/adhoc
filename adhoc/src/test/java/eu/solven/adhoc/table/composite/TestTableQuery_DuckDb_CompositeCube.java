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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.UnsafeMeasureForestBag;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.adhoc.util.NotYetImplementedException;

public class TestTableQuery_DuckDb_CompositeCube extends ADagTest implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	Aggregator k3Sum = Aggregator.builder().name("k3").aggregationKey(SumAggregation.KEY).build();

	DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();
	DSLContext dsl = dslSupplier.getDSLContext();

	String tableName1 = "someTableName1";
	JooqTableWrapper table1 = new JooqTableWrapper(tableName1,
			JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName1).build());

	String tableName2 = "someTableName2";
	JooqTableWrapper table2 = new JooqTableWrapper(tableName2,
			JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName2).build());

	@Override
	public void feedTable() {
		// no feeding by default
	}

	private CubeWrapper wrapInCube(IMeasureForest forest, JooqTableWrapper table) {
		return CubeWrapper.builder().name(table.getName() + ".cube").engine(engine).forest(forest).table(table).build();
	}

	private CubeWrapper makeAndFeedCompositeCube() {
		CubeWrapper cube1;
		{
			dsl.createTableIfNotExists(tableName1)
					.column("k1", SQLDataType.DOUBLE)
					.column("k2", SQLDataType.DOUBLE)
					.column("a", SQLDataType.VARCHAR)
					.column("b", SQLDataType.VARCHAR)
					.execute();
			dsl.insertInto(DSL.table(tableName1), DSL.field("k1"), DSL.field("k2"), DSL.field("a"), DSL.field("b"))
					.values(123, 234, "a1", "b1")
					.execute();
			dsl.insertInto(DSL.table(tableName1), DSL.field("k1"), DSL.field("k2"), DSL.field("a"), DSL.field("b"))
					.values(345, 456, "a2", "b2")
					.execute();
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k2Sum);
			measureBag.addMeasure(Aggregator.countAsterisk());
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			dsl.createTableIfNotExists(tableName2)
					.column("k1", SQLDataType.DOUBLE)
					.column("k3", SQLDataType.DOUBLE)
					.column("a", SQLDataType.VARCHAR)
					.column("c", SQLDataType.VARCHAR)
					.execute();
			dsl.insertInto(DSL.table(tableName2), DSL.field("k1"), DSL.field("k3"), DSL.field("a"), DSL.field("c"))
					.values(1234, 2345, "a1", "c1")
					.execute();
			UnsafeMeasureForestBag measureBag = UnsafeMeasureForestBag.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k3Sum);
			measureBag.addMeasure(Aggregator.countAsterisk());
			cube2 = wrapInCube(measureBag, table2);
		}

		UnsafeMeasureForestBag measureBagWithoutUnderlyings =
				UnsafeMeasureForestBag.builder().name("composite").build();
		measureBagWithoutUnderlyings.addMeasure(k1PlusK2AsExpr);

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		IMeasureForest measureBagWithUnderlyings =
				compositeCubesTable.injectUnderlyingMeasures(measureBagWithoutUnderlyings);

		CubeWrapper cube3 = CubeWrapper.builder()
				.engine(engine)
				.forest(measureBagWithUnderlyings)
				.table(compositeCubesTable)
				.build();
		return cube3;
	}

	@Test
	public void testGetColumns() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		Assertions.assertThat(cube3.getColumns())
				.containsEntry("a", String.class)
				.containsEntry("b", String.class)
				.containsEntry("c", String.class)
				.containsEntry(k1Sum.getColumnName(), Double.class)
				.containsEntry(k2Sum.getColumnName(), Double.class)
				.containsEntry(k3Sum.getColumnName(), Double.class)
				.hasSize(6);

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

			ITabularView result = cube3.execute(AdhocQuery.builder().measure(k2Sum.getName()).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k2Sum.getName(), 0L + 234 + 456))
					.hasSize(1);
		}

		// `k2` does not exists in cube2 and `a` is in both cubes
		{
			ITabularView result = cube3.execute(AdhocQuery.builder().measure(k2Sum.getName()).groupByAlso("a").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("a", "a1"), Map.of(k2Sum.getName(), 0L + 234))
					.containsEntry(Map.of("a", "a2"), Map.of(k2Sum.getName(), 0L + 456))
					.hasSize(2);
		}

		// `k2` does not exists in cube2 and `b` does not exists in cube2
		{
			ITabularView result = cube3.execute(AdhocQuery.builder().measure(k2Sum.getName()).groupByAlso("b").build());
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
			ITabularView result = cube3.execute(AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of(), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 345 + 456 + 1234))
					.hasSize(1);
		}

		// `a` is in both cubes
		{
			ITabularView result =
					cube3.execute(AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("a").build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(result);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.containsEntry(Map.of("a", "a1"), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 1234))
					.containsEntry(Map.of("a", "a2"), Map.of(k1PlusK2AsExpr.getName(), 0L + 345 + 456))
					.hasSize(2);
		}

		// `b` does not exists in cube2
		{
			ITabularView result =
					cube3.execute(AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("b").build());
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
				cube3.execute(AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("a").build());
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
				cube3.execute(AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("b").build());
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
				cube3.execute(AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).andFilter("a", "a1").build());
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
				AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).andFilter("b", "b1").explain(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 1234))
				.hasSize(1);

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualTo(
						"""
								#0 s=composite id=00000000-0000-0000-0000-000000000000
								\\-- #1 m=k1PlusK2AsExpr(Combinator) filter=b=b1 groupBy=grandTotal
								    |\\- #2 m=k1(Aggregator) filter=b=b1 groupBy=grandTotal
								    \\-- #3 m=k2(Aggregator) filter=b=b1 groupBy=grandTotal
								#0 s=someTableName1 id=00000000-0000-0000-0000-000000000001 (parentId=00000000-0000-0000-0000-000000000000)
								|\\- #1 m=k1(Aggregator) filter=b=b1 groupBy=grandTotal
								\\-- #2 m=k2(Aggregator) filter=b=b1 groupBy=grandTotal
								#0 s=someTableName2 id=00000000-0000-0000-0000-000000000002 (parentId=00000000-0000-0000-0000-000000000000)
								\\-- #1 m=k1(Aggregator) filter=matchAll groupBy=grandTotal"""
								.trim());
		Assertions.assertThat(messages).hasSize(9);
	}

	@Test
	public void testQuery_Count() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(AdhocQuery.builder().measure(Aggregator.countAsterisk()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(Aggregator.countAsterisk().getName(), 0L + 2 + 1))
				.hasSize(1);
	}

	// Make sure Composite can not only do Linear SUMs
	@Test
	public void testQuery_Max() {
		CubeWrapper initialCube3 = makeAndFeedCompositeCube();

		// We add k1Min and k1Max in the composite cube: these measures are not known from the underlying cubes.
		CubeWrapper cube3 = initialCube3.toBuilder()
				.forest(MeasureForest.edit(initialCube3.getForest())
						.measure(Aggregator.builder()
								.name("k1Min")
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

		ITabularView result = cube3.execute(AdhocQuery.builder().measure("k1Min", "k1Max").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of("k1Min", 123D, "k1Max", 1234D))
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
						.build())
				.build();

		Assertions
				.assertThatThrownBy(
						() -> cube3.execute(AdhocQuery.builder().measure("k1Rank1", "k1Rank2", "k1Rank3").build()))
				.hasRootCauseInstanceOf(NotYetImplementedException.class);
		// ITabularView result = cube3.execute(AdhocQuery.builder().measure("k1Rank1", "k1Rank2", "k1Rank3").build());
		// MapBasedTabularView mapBased = MapBasedTabularView.load(result);
		//
		// Assertions.assertThat(mapBased.getCoordinatesToValues())
		// .containsEntry(Map.of(), Map.of("k1Rank1", 1234D, "k1Rank2", 234D, "k1Rank3", 123D))
		// .hasSize(1);
	}

	@Test
	public void testQuery_Avg() {
		CubeWrapper initialCube3 = makeAndFeedCompositeCube();

		// We add k1Min and k1Max in the composite cube: these measures are not known from the underlying cubes.
		CubeWrapper cube3 = initialCube3.toBuilder()
				.forest(MeasureForest.edit(initialCube3.getForest())
						.measure(Aggregator.builder()
								.name("k1.avg")
								.columnName("k1")
								.aggregationKey(AvgAggregation.KEY)
								.build())
						.build())
				.build();

		Assertions.assertThatThrownBy(() -> cube3.execute(AdhocQuery.builder().measure("k1.avg").build()))
				.hasRootCauseInstanceOf(NotYetImplementedException.class);

		// ITabularView result = cube3.execute(AdhocQuery.builder().measure("k1.avg").build());
		// MapBasedTabularView mapBased = MapBasedTabularView.load(result);
		//
		// Assertions.assertThat(mapBased.getCoordinatesToValues())
		// .containsEntry(Map.of(), Map.of("k1.avg", 1234D))
		// .hasSize(1);
	}

	@Test
	public void testQueryNoMeasures_common() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(AdhocQuery.builder().groupByAlso("a").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of())
				.containsEntry(Map.of("a", "a2"), Map.of())
				.hasSize(2);
	}

	@Test
	public void testQueryNoMeasures_onlyOne() {
		CubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(AdhocQuery.builder().groupByAlso("b", "c").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("b", "b1", "c", "someTableName1.cube"), Map.of())
				.containsEntry(Map.of("b", "b2", "c", "someTableName1.cube"), Map.of())
				.containsEntry(Map.of("b", "someTableName2.cube", "c", "c1"), Map.of())
				.hasSize(3);
	}

	AtomicInteger stopWatchCount = new AtomicInteger();

	@Override
	public IStopwatch makeStopwatch() {
		int stopWatchIndex = stopWatchCount.incrementAndGet();

		return () -> Duration.ofMillis(123 * stopWatchIndex);
	}

	@Test
	public void testLogPerfs() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

		CubeWrapper cube3 = makeAndFeedCompositeCube();

		cube3.execute(AdhocQuery.builder()
				.measure("k1", "k2", "k3")
				.customMarker(Optional.of("JPY"))
				.groupByAlso("letter")
				.andFilter("color", "red")
				.explain(true)
				.build());

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualToNormalizingNewlines(
						"""
								#0 s=someTableName1 id=00000000-0000-0000-0000-000000000001 (parentId=00000000-0000-0000-0000-000000000000)
								|  No cost info
								|\\- #1 m=k1(Aggregator) filter=matchAll groupBy=grandTotal customMarker=JPY
								|   \\  size=1 duration=492ms
								\\-- #2 m=k2(Aggregator) filter=matchAll groupBy=grandTotal customMarker=JPY
								    \\  size=1 duration=492ms
								Executed status=OK duration=PT0.369S on table=someTableName1 measures=someTableName1 query=AdhocSubQuery(subQuery=AdhocQuery(filter=matchAll, groupBy=grandTotal, measures=[ReferencedMeasure(ref=k1), ReferencedMeasure(ref=k2)], customMarker=JPY, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED]), parentQueryId=AdhocQueryId(queryIndex=0, queryId=00000000-0000-0000-0000-000000000000, parentQueryId=null, queryHash=321c99e0, cube=composite))
								#0 s=someTableName2 id=00000000-0000-0000-0000-000000000002 (parentId=00000000-0000-0000-0000-000000000000)
								|  No cost info
								|\\- #1 m=k1(Aggregator) filter=matchAll groupBy=grandTotal customMarker=JPY
								|   \\  size=1 duration=738ms
								\\-- #2 m=k3(Aggregator) filter=matchAll groupBy=grandTotal customMarker=JPY
								    \\  size=1 duration=738ms
								Executed status=OK duration=PT0.615S on table=someTableName2 measures=someTableName2 query=AdhocSubQuery(subQuery=AdhocQuery(filter=matchAll, groupBy=grandTotal, measures=[ReferencedMeasure(ref=k1), ReferencedMeasure(ref=k3)], customMarker=JPY, options=[EXPLAIN, UNKNOWN_MEASURES_ARE_EMPTY, AGGREGATION_CARRIERS_STAY_WRAPPED]), parentQueryId=AdhocQueryId(queryIndex=0, queryId=00000000-0000-0000-0000-000000000000, parentQueryId=null, queryHash=321c99e0, cube=composite))
								#0 s=composite id=00000000-0000-0000-0000-000000000000
								|  No cost info
								|\\- #1 m=k1(Aggregator) filter=color=red groupBy=(letter) customMarker=JPY
								|   \\  size=2 duration=246ms
								|\\- #2 m=k2(Aggregator) filter=color=red groupBy=(letter) customMarker=JPY
								|   \\  size=1 duration=246ms
								\\-- #3 m=k3(Aggregator) filter=color=red groupBy=(letter) customMarker=JPY
								    \\  size=1 duration=246ms
								Executed status=OK duration=PT0.123S on table=composite measures=composite query=AdhocQuery(filter=color=red, groupBy=(letter), measures=[ReferencedMeasure(ref=k1), ReferencedMeasure(ref=k2), ReferencedMeasure(ref=k3)], customMarker=JPY, options=[EXPLAIN])""");

		Assertions.assertThat(messages).hasSize(13);
	}
}
