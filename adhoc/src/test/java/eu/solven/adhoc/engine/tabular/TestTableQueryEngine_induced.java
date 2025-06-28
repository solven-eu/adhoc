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
package eu.solven.adhoc.engine.tabular;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.IValueProviderTestHelpers;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.TableQueryEngine.SplitTableQueries;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;

public class TestTableQueryEngine_induced extends ADagTest implements IAdhocTestConstants {

	TableQueryEngine tableQueryEngine = engine().makeTableQueryEngine();

	@Override
	public void feedTable() {
		// no need for data
	}

	@Test
	public void test_sum_ByCcyAndGrandTotal() {
		forest.addMeasure(Partitionor.builder()
				.name("byCcy")
				.underlyings(Arrays.asList(k1Sum.getName()))
				.combinationKey(SumCombination.KEY)
				.groupBy(GroupByColumns.named("ccy"))
				.build());

		forest.addMeasure(k1Sum);

		CubeQuery cubeQuery = CubeQuery.builder().measure("byCcy", k1Sum.getName()).build();
		QueryPod queryPod = QueryPod.builder().query(cubeQuery).forest(forest).table(table()).build();

		Set<TableQuery> output = tableQueryEngine.prepareForTable(queryPod, engine().makeQueryStepsDag(queryPod));
		Assertions.assertThat(output).hasSize(2);

		SplitTableQueries split = tableQueryEngine.splitInduced(queryPod, output);

		Assertions.assertThat(split.getInducers())
				.contains(CubeQueryStep.edit(cubeQuery).groupBy(GroupByColumns.named("ccy")).measure(k1Sum).build())
				.hasSize(1);

		Assertions.assertThat(split.getInduceds())
				.contains(CubeQueryStep.edit(cubeQuery).measure(k1Sum).build())
				.hasSize(1);

		{
			IMultitypeColumnFastGet<SliceAsMap> columnFromTable = MultitypeHashColumn.<SliceAsMap>builder().build();
			columnFromTable.append(SliceAsMap.fromMap(Map.of("ccy", "EUR"))).onLong(123);
			columnFromTable.append(SliceAsMap.fromMap(Map.of("ccy", "USD"))).onLong(234);

			ISliceToValue valuesFromTable = SliceToValue.builder().column(columnFromTable).build();
			Map<CubeQueryStep, ISliceToValue> fromTable = new ConcurrentHashMap<>();
			fromTable.put(CubeQueryStep.edit(cubeQuery).groupBy(GroupByColumns.named("ccy")).measure(k1Sum).build(),
					valuesFromTable);
			tableQueryEngine.evaluateImplicit(queryPod, fromTable, split);

			Assertions.assertThat(fromTable)
					// inducer
					.containsEntry(
							CubeQueryStep.edit(cubeQuery).groupBy(GroupByColumns.named("ccy")).measure(k1Sum).build(),
							valuesFromTable)
					// induced
					.hasEntrySatisfying(
							CubeQueryStep.edit(cubeQuery).groupBy(IAdhocGroupBy.GRAND_TOTAL).measure(k1Sum).build(),
							t -> {
								Assertions.assertThat(t.size()).isEqualTo(1);
								Assertions
										.assertThat(IValueProviderTestHelpers
												.getLong(t.onValue(SliceAsMap.fromMap(Map.of()))))
										.isEqualTo(0L + 123 + 234);
							})
					.hasSize(2);
		}
	}

	@Test
	public void test_sum_ByCcyCountryAndByCcyAndGrandTotal() {
		forest.addMeasure(Partitionor.builder()
				.name("byCcyCountry")
				.underlyings(Arrays.asList(k1Sum.getName()))
				.combinationKey(SumCombination.KEY)
				.groupBy(GroupByColumns.named("ccy", "country"))
				.build());

		forest.addMeasure(Partitionor.builder()
				.name("byCcy")
				.underlyings(Arrays.asList(k1Sum.getName()))
				.combinationKey(SumCombination.KEY)
				.groupBy(GroupByColumns.named("ccy"))
				.build());

		forest.addMeasure(k1Sum);

		CubeQuery cubeQuery =
				CubeQuery.builder().measure("byCcyCountry", "byCcy", k1Sum.getName()).explain(true).build();
		QueryPod queryPod = QueryPod.builder().query(cubeQuery).forest(forest).table(table()).build();

		Set<TableQuery> output = tableQueryEngine.prepareForTable(queryPod, engine().makeQueryStepsDag(queryPod));
		Assertions.assertThat(output).hasSize(3);

		SplitTableQueries split = tableQueryEngine.splitInduced(queryPod, output);

		Assertions.assertThat(split.getInducers())
				.contains(CubeQueryStep.edit(cubeQuery)
						.groupBy(GroupByColumns.named("ccy", "country"))
						.measure(k1Sum)
						.build())
				.hasSize(1);

		Assertions.assertThat(split.getInduceds())
				.contains(CubeQueryStep.edit(cubeQuery).measure(k1Sum).build())
				.contains(CubeQueryStep.edit(cubeQuery).groupBy(GroupByColumns.named("ccy")).measure(k1Sum).build())
				.hasSize(2);

		{
			IMultitypeColumnFastGet<SliceAsMap> columnFromTable = MultitypeHashColumn.<SliceAsMap>builder().build();
			columnFromTable.append(SliceAsMap.fromMap(Map.of("ccy", "EUR", "country", "France"))).onLong(123);
			columnFromTable.append(SliceAsMap.fromMap(Map.of("ccy", "EUR", "country", "Germany"))).onLong(234);
			columnFromTable.append(SliceAsMap.fromMap(Map.of("ccy", "USD", "country", "USA"))).onLong(345);

			ISliceToValue valuesFromTable = SliceToValue.builder().column(columnFromTable).build();
			Map<CubeQueryStep, ISliceToValue> fromTable = new ConcurrentHashMap<>();
			fromTable.put(CubeQueryStep.edit(cubeQuery)
					.groupBy(GroupByColumns.named("ccy", "country"))
					.measure(k1Sum)
					.build(), valuesFromTable);
			tableQueryEngine.evaluateImplicit(queryPod, fromTable, split);

			Assertions.assertThat(fromTable)
					// inducer
					.containsEntry(
							CubeQueryStep.edit(cubeQuery)
									.groupBy(GroupByColumns.named("ccy", "country"))
									.measure(k1Sum)
									.build(),
							valuesFromTable)
					// induced by ccy
					.hasEntrySatisfying(
							CubeQueryStep.edit(cubeQuery).groupBy(GroupByColumns.named("ccy")).measure(k1Sum).build(),
							t -> {
								Assertions.assertThat(t.size()).isEqualTo(2);
								Assertions
										.assertThat(IValueProviderTestHelpers
												.getLong(t.onValue(SliceAsMap.fromMap(Map.of("ccy", "EUR")))))
										.isEqualTo(0L + 123 + 234);
								Assertions
										.assertThat(IValueProviderTestHelpers
												.getLong(t.onValue(SliceAsMap.fromMap(Map.of("ccy", "USD")))))
										.isEqualTo(0L + 345);
							})
					// induced grandTotal
					.hasEntrySatisfying(
							CubeQueryStep.edit(cubeQuery).groupBy(IAdhocGroupBy.GRAND_TOTAL).measure(k1Sum).build(),
							t -> {
								Assertions.assertThat(t.size()).isEqualTo(1);
								Assertions
										.assertThat(IValueProviderTestHelpers
												.getLong(t.onValue(SliceAsMap.fromMap(Map.of()))))
										.isEqualTo(0L + 123 + 234 + 345);
							})
					.hasSize(3);
		}
	}
}
