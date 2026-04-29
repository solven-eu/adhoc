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
package eu.solven.adhoc.table.duckdb.perf;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.encoding.column.AdhocColumnUnsafe;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.ProductCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.TestTransformator_Combinator_Perf;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.cache.CachingTableWrapper;
import eu.solven.adhoc.table.duckdb.ADuckDbJooqTest;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import eu.solven.adhoc.util.AdhocBenchmark;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.IStopwatchFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Similar to {@link TestTransformator_Combinator_Perf}, but based on DuckDb.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@AdhocBenchmark
public class TestTransformator_Partitionor_Perf_DuckDb extends ADuckDbJooqTest implements IAdhocTestConstants {
	static final int maxCardinality = 1_000_000 / 1;

	@BeforeAll
	public static void setLimits() {
		log.info("Evaluating on cardinality={}", maxCardinality);
		AdhocColumnUnsafe.setLimitColumnSize(maxCardinality + 10);
	}

	@AfterAll
	public static void resetLimits() {
		AdhocColumnUnsafe.resetProperties();
	}

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		dsl.execute("""
				CREATE OR REPLACE TABLE %s (l VARCHAR, row_index INTEGER, k1 INTEGER, k2 INTEGER);
				INSERT INTO %s (
				    SELECT
				    	'A',
				        i,
				        i,
				        i %% 9
				    FROM range(%s) AS t(i)
				);
								""".formatted(tableName, tableName, maxCardinality));
		return new JooqTableWrapper(tableName,
				DuckDBHelper.parametersBuilder(dslSupplier).tableName(tableName).build());
	}

	@Override
	public IStopwatchFactory makeStopwatchFactory() {
		return IStopwatchFactory.guavaStopwatchFactory();
	}

	String m = "sum_K1xK2_byRowIndex";

	@BeforeEach
	public void registerMeasures() {
		forest.addMeasure(Partitionor.builder()
				.name(m)
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(ProductCombination.KEY)
				.aggregationKey(SumAggregation.KEY)
				.groupBy(GroupByColumns.named("row_index"))
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);
	}

	@Test
	public void testGrandTotal_Sequential() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBusGuava());

		long sum = LongStream.range(0, maxCardinality).map(i -> i * (i % 9)).sum();

		ITabularView output = cube().execute(CubeQuery.builder().measure(m).groupByAlso("l").explain(true).build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("l", "A"), Map.of(m, sum));

		log.info("Performance report:{}{}", "\r\n", String.join("\r\n", messages));
	}

	@Test
	public void testGrandTotal_Concurrent() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBusGuava());

		long sum = LongStream.range(0, maxCardinality).map(i -> i * (i % 9)).sum();

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(m)
				.groupByAlso("l")
				.option(StandardQueryOptions.CONCURRENT)
				.explain(true)
				.build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("l", "A"), Map.of(m, sum));

		log.info("Performance report:{}{}", "\r\n", String.join("\r\n", messages));
	}

	@Test
	public void testGrandTotal_Concurrent_Partitioned() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBusGuava());

		long sum = LongStream.range(0, maxCardinality).map(i -> i * (i % 9)).sum();

		AdhocUnsafe.setParallelism(2);
		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(m)
				.groupByAlso("l")
				.option(StandardQueryOptions.CONCURRENT)
				.option(StandardQueryOptions.PARTITIONED)
				.explain(true)
				.build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("l", "A"), Map.of(m, sum));

		log.info("Performance report:{}{}", "\r\n", String.join("\r\n", messages));
	}

	@Test
	public void testGrandTotal_Sequential_withCache() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBusGuava());

		long sum = LongStream.range(0, maxCardinality).map(i -> i * (i % 9)).sum();

		CubeWrapper cubeWithCache = CubeWrapper.builder()
				.table(CachingTableWrapper.builder().decorated(table()).build())
				.engine(engine())
				.forest(forest)
				.eventBus(eventBus())
				.build();

		// Fill the cache
		messages.clear();
		cubeWithCache.execute(CubeQuery.builder().measure(m).groupByAlso("l").explain(true).build());
		log.info("[PREFILL] Performance report:{}{}", "\r\n", String.join("\r\n", messages));

		log.info("---Cache is filled---");

		messages.clear();
		ITabularView output = withCache(cubeWithCache);

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("l", "A"), Map.of(m, sum));

		log.info("[FILLED] Performance report:{}{}", "\r\n", String.join("\r\n", messages));
	}

	// BEWARE Wrapped in a subMethod in order to clearly separate this leg in profiling stacks
	private ITabularView withCache(CubeWrapper cubeWithCache) {
		return cubeWithCache.execute(CubeQuery.builder().measure(m).groupByAlso("l").explain(true).build());
	}

}
