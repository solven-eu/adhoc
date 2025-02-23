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
package eu.solven.adhoc.composite;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import eu.solven.adhoc.table.CompositeCubesTableWrapper;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapper;
import eu.solven.adhoc.table.sql.AdhocJooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.DuckDbHelper;

public class TestTableQuery_CompositeCube implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	Aggregator k3Sum = Aggregator.builder().name("k3").aggregationKey(SumAggregation.KEY).build();

	EventBus eventBus = AdhocTestHelper.eventBus();

	// We can re-use a single engine for each cubes
	AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(eventBus::post).build();

	DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();
	DSLContext dsl = dslSupplier.getDSLContext();

	String tableName1 = "someTableName1";
	AdhocJooqTableWrapper table1 = new AdhocJooqTableWrapper(tableName1,
			AdhocJooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName1).build());

	String tableName2 = "someTableName2";
	AdhocJooqTableWrapper table2 = new AdhocJooqTableWrapper(tableName2,
			AdhocJooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName2).build());

	private AdhocCubeWrapper wrapInCube(AdhocMeasureBag measureBag, AdhocJooqTableWrapper table) {
		return AdhocCubeWrapper.builder()
				.name(table.getName() + ".cube")
				.engine(aqe)
				.measures(measureBag)
				.table(table)
				.engine(aqe)
				.build();
	}

	private AdhocCubeWrapper makeAndFeedCompositeCube() {
		AdhocCubeWrapper cube1;
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
			AdhocMeasureBag measureBag = AdhocMeasureBag.builder().name(tableName1).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k2Sum);
			cube1 = wrapInCube(measureBag, table1);
		}
		AdhocCubeWrapper cube2;
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
			AdhocMeasureBag measureBag = AdhocMeasureBag.builder().name(tableName2).build();
			measureBag.addMeasure(k1Sum);
			measureBag.addMeasure(k3Sum);
			cube2 = wrapInCube(measureBag, table2);
		}

		AdhocMeasureBag measureBag = AdhocMeasureBag.builder().name("composite").build();
		measureBag.addMeasure(k1PlusK2AsExpr);

		AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(eventBus::post).build();
		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		compositeCubesTable.injectUnderlyingMeasures(measureBag);

		AdhocCubeWrapper cube3 = AdhocCubeWrapper.builder()
				.engine(aqe)
				.measures(measureBag)
				.table(compositeCubesTable)
				.engine(aqe)
				.build();
		return cube3;
	}

	@Test
	public void testQueryCube1() {
		AdhocCubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(AdhocQuery.builder().measure(k2Sum.getName()).debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k2Sum.getName(), 0L + 234 + 456))
				.hasSize(1);

		Assertions.assertThat(cube3.getColumns())
				.containsEntry("a", String.class)
				.containsEntry("b", String.class)
				.containsEntry("c", String.class)
				.containsEntry(k1Sum.getColumnName(), Double.class)
				.containsEntry(k1PlusK2AsExpr.getName(), Double.class)
				.containsEntry(k2Sum.getColumnName(), Double.class)
				.containsEntry(k3Sum.getColumnName(), Double.class)
				.hasSize(7);
	}

	@Test
	public void testQueryCube1Plus2() {
		AdhocCubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 345 + 456 + 1234))
				.hasSize(1);
	}

	@Test
	public void testQueryCube1Plus2_groupByShared() {
		AdhocCubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3
				.execute(AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("a").debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 1234))
				.containsEntry(Map.of("a", "a2"), Map.of(k1PlusK2AsExpr.getName(), 0L + 345 + 456))
				.hasSize(2);
	}

	@Test
	public void testQueryCube1Plus2_groupByUnshared() {
		AdhocCubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3
				.execute(AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).groupByAlso("b").debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("b", "b1"), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234))
				.containsEntry(Map.of("b", "b2"), Map.of(k1PlusK2AsExpr.getName(), 0L + 345 + 456))
				.containsEntry(Map.of("b", "someTableName2.cube"), Map.of(k1PlusK2AsExpr.getName(), 0L + 1234))
				.hasSize(3);
	}

	@Test
	public void testQueryCube1Plus2_filterShared() {
		AdhocCubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(
				AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).andFilter("a", "a1").debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 1234))
				.hasSize(1);
	}

	@Test
	public void testQueryCube1Plus2_filterUnshared() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplain(eventBus);

		AdhocCubeWrapper cube3 = makeAndFeedCompositeCube();

		ITabularView result = cube3.execute(
				AdhocQuery.builder().measure(k1PlusK2AsExpr.getName()).andFilter("b", "b1").debug(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 1234))
				.hasSize(1);

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				#0 m=k1PlusK2AsExpr(Combinator) filter=b=b1 groupBy=grandTotal
				|\\- #1 m=k1(Aggregator) filter=b=b1 groupBy=grandTotal
				\\-- #2 m=k2(Aggregator) filter=b=b1 groupBy=grandTotal
				#0 m=k1(Aggregator) filter=b=b1 groupBy=grandTotal
				#1 m=k2(Aggregator) filter=b=b1 groupBy=grandTotal
				#0 m=k1(Aggregator) filter=matchAll groupBy=grandTotal""".trim());
		Assertions.assertThat(messages).hasSize(6);
	}
}
