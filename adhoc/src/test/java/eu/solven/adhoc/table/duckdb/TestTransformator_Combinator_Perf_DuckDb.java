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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import com.google.common.math.LongMath;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.measure.transformator.TestTransformator_Combinator_Perf;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.worldcup.TestAdhocIntegrationTests;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import lombok.extern.slf4j.Slf4j;

/**
 * Similar to {@link TestTransformator_Combinator_Perf}, but based on DuckDb.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@EnabledIf(TestAdhocIntegrationTests.ENABLED_IF)
@Tag("adhoc-benchmark")
public class TestTransformator_Combinator_Perf_DuckDb extends ADuckDbJooqTest implements IAdhocTestConstants {
	int maxCardinality = 10_000;

	// This will be edited by registerMeasures
	String timesNMinus1 = k1Sum.getName();
	// This will be edited by registerMeasures
	String timesN = k1Sum.getName();

	final int height = 16;

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		dsl.execute("""
				CREATE OR REPLACE TABLE %s (l VARCHAR, row_index INTEGER, k1 INTEGER);
				INSERT INTO %s (
				    SELECT
				    	'A',
				        i,
				        i
				    FROM range(%s) AS t(i)
				);
								""".formatted(tableName, tableName, maxCardinality));

		return new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder().dslSupplier(dslSupplier).tableName(tableName).build());
	}

	@BeforeEach
	public void registerMeasures() {
		forest.addMeasure(k1Sum);

		for (int i = 0; i < height; i++) {
			if (i == 0) {
				timesNMinus1 = k1Sum.getName();
			} else {
				timesNMinus1 = k1Sum.getName() + "x" + (1 << i);
			}
			timesN = k1Sum.getName() + "x" + (1 << (i + 1));

			forest.addMeasure(Combinator.builder()
					.name(timesN)
					.underlyings(Arrays.asList(timesNMinus1, timesNMinus1))
					.combinationKey(SumCombination.KEY)
					.build());
		}
	}

	@Test
	public void testGrandTotal() {
		// SUM(0..N) = N * (N-1) / 2
		long sum = LongMath.checkedMultiply(maxCardinality, maxCardinality - 1) / 2;

		ITabularView output = cube().execute(CubeQuery.builder().measure(timesN).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of(timesN, 0L + sum * (1L << height)));
	}

	@Test
	public void testChainOfSums() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBusGuava());

		ITabularView output =
				cube().execute(CubeQuery.builder().measure(timesN).groupByAlso("row_index").explain(true).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(maxCardinality)
				.containsEntry(Map.of("row_index", 0L), Map.of(timesN, 0L))
				.containsEntry(Map.of("row_index", 1L), Map.of(timesN, 0L + (1L << height)))
				.containsEntry(Map.of("row_index", 0L + maxCardinality - 1),
						Map.of(timesN, 0L + (maxCardinality - 1) * (1L << height)));

		log.info("Performance report:{}{}", "\r\n", messages.stream().collect(Collectors.joining("\r\n")));
	}

	// Check that EmptyAggregation is fast
	@Test
	public void testNoMeasures() {
		List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBusGuava());

		ITabularView output = cube().execute(CubeQuery.builder().groupByAlso("row_index").explain(true).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(maxCardinality)
				.containsEntry(Map.of("row_index", 0L), Map.of())
				.containsEntry(Map.of("row_index", 1L), Map.of())
				.containsEntry(Map.of("row_index", 0L + maxCardinality - 1), Map.of());

		log.info("Performance report:{}{}", "\r\n", messages.stream().collect(Collectors.joining("\r\n")));
	}
}
