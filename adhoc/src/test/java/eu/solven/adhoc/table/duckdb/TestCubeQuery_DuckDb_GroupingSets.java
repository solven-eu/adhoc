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
package eu.solven.adhoc.table.duckdb;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import ch.qos.logback.classic.LoggerContext;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.eventbus.UnsafeAdhocEventBusHelpers;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;

public class TestCubeQuery_DuckDb_GroupingSets extends ADuckDbJooqTest implements IAdhocTestConstants {
	String tableName = "someTableName";

	static {
		// https://stackoverflow.com/questions/59491564/logback-doesnt-print-method-or-line-number
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		List<String> frameworkPackages = loggerContext.getFrameworkPackages();
		UnsafeAdhocEventBusHelpers.addToFrameworkPackages(frameworkPackages);
	}

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName).build());
	}

	@Test
	public void test_grandTotal_measureByA() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		Aggregator sumK1 = Aggregator.sum("k1");
		forest.addMeasure(sumK1);

		Partitionor sumK1ByA = Partitionor.builder()
				.name("byA")
				.underlying(sumK1.getName())
				.groupBy(GroupByColumns.named("a"))
				.build();

		ITabularView result = cube().execute(CubeQuery.builder().measure(sumK1, sumK1ByA).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat((Map) mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(),
						ImmutableMap.builder()
								.put(sumK1.getName(), 0D + 123 + 234 + 345)
								.put(sumK1ByA.getName(), 0D + 123 + 234 + 345)
								.build());
	}

	@Test
	public void test_grandTotal_measureByA_groupByB() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a1", "b1", 123)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a2", "b2", 234)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a1", "b2", 345)
				.execute();

		Aggregator sumK1 = Aggregator.sum("k1");
		forest.addMeasure(sumK1);

		Partitionor sumK1ByA = Partitionor.builder()
				.name("byA")
				.underlying(sumK1.getName())
				.groupBy(GroupByColumns.named("b"))
				.build();

		ITabularView result = cube().execute(CubeQuery.builder().measure(sumK1, sumK1ByA).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat((Map) mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(),
						ImmutableMap.builder()
								.put(sumK1.getName(), 0D + 123 + 234 + 345)
								.put(sumK1ByA.getName(), 0D + 123 + 234 + 345)
								.build());
	}

	@Test
	public void test_grandTotal_measureByA_qualifiedColumn() {
		dsl.createTableIfNotExists(tableName)
				.column("a.b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.name("a.b")), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.name("a.b")), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.name("a.b")), DSL.field("k1")).values("a1", 345).execute();

		Aggregator sumK1 = Aggregator.sum("k1");
		forest.addMeasure(sumK1);

		Partitionor sumK1ByA = Partitionor.builder()
				.name("byA")
				.underlying(sumK1.getName())
				.groupBy(GroupByColumns.named("\"a.b\""))
				.build();

		ITabularView result = cube().execute(CubeQuery.builder().measure(sumK1, sumK1ByA).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat((Map) mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(),
						ImmutableMap.builder()
								.put(sumK1.getName(), 0D + 123 + 234 + 345)
								.put(sumK1ByA.getName(), 0D + 123 + 234 + 345)
								.build());
	}

	@Test
	public void test_grandTotal_groupByA_spaceColumn() {
		dsl.createTableIfNotExists(tableName)
				.column("a b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.name("a b")), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.name("a b")), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field(DSL.name("a b")), DSL.field("k1")).values("a1", 345).execute();

		Aggregator sumK1 = Aggregator.sum("k1");
		forest.addMeasure(sumK1);

		Partitionor sumK1ByA = Partitionor.builder()
				.name("byA")
				.underlying(sumK1.getName())
				.groupBy(GroupByColumns.named("a b"))
				.build();

		ITabularView result = cube().execute(CubeQuery.builder().measure(sumK1, sumK1ByA).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat((Map) mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(),
						ImmutableMap.builder()
								.put(sumK1.getName(), 0D + 123 + 234 + 345)
								.put(sumK1ByA.getName(), 0D + 123 + 234 + 345)
								.build());
	}
}
