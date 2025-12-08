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

import ch.qos.logback.classic.LoggerContext;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.util.UnsafeAdhocEventBusHelpers;

public class TestCubeQuery_DuckDb_Aggregations extends ADuckDbJooqTest implements IAdhocTestConstants {
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
	public void test_Rank_grandTotal() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		Aggregator k1Rank2 = Aggregator.builder()
				.name("k1")
				.aggregationKey(RankAggregation.KEY)
				.aggregationOption(RankAggregation.P_RANK, 2)
				.build();
		forest.addMeasure(k1Rank2);

		ITabularView result = cube().execute(CubeQuery.builder().measure(k1Rank2).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Rank2.getName(), 234D));
	}

	@Test
	public void test_Rank_groupBy() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 345).execute();

		Aggregator k1Rank2 = Aggregator.builder()
				.name("k1")
				.aggregationKey(RankAggregation.KEY)
				.aggregationOption(RankAggregation.P_RANK, 2)
				.build();
		forest.addMeasure(k1Rank2);

		ITabularView result = cube().execute(CubeQuery.builder().groupByAlso("a").measure(k1Rank2).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of(k1Rank2.getName(), 123D))
				.hasSize(1);
	}
}
