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
package eu.solven.adhoc.engine;

import java.util.Arrays;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.engine.TableQueryEngine;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.InMemoryTable;

public class TestPrepareTableQuery extends ADagTest implements IAdhocTestConstants {

	ITableWrapper table = InMemoryTable.builder().build();
	TableQueryEngine tableQueryEngine = engine.makeTableQueryEngine();

	@Override
	public void feedTable() {
		// no need for data
	}

	@Test
	public void testSum() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		QueryPod executingQueryContext = QueryPod.builder()
				.query(CubeQuery.builder().measure(k1Sum).build())
				.forest(forest)
				.table(table)
				.build();
		Set<TableQuery> output = tableQueryEngine.prepareForTable(executingQueryContext,
				engine.makeQueryStepsDag(executingQueryContext));

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getAggregators()).hasSize(1).contains(k1Sum);
		});
	}

	@Test
	public void testSumOfSum() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		QueryPod executingQueryContext = QueryPod.builder()
				.query(CubeQuery.builder().measure("sumK1K2").build())
				.forest(forest)
				.table(table)
				.build();
		Set<TableQuery> output = tableQueryEngine.prepareForTable(executingQueryContext,
				engine.makeQueryStepsDag(executingQueryContext));

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getAggregators()).hasSize(2).contains(k1Sum, k2Sum);
		});
	}

	@Test
	public void testSum_SumOfSum() {
		forest.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		QueryPod executingQueryContext = QueryPod.builder()
				.query(CubeQuery.builder().measure("sumK1K2").build())
				.forest(forest)
				.table(table)
				.build();
		Set<TableQuery> output = tableQueryEngine.prepareForTable(executingQueryContext,
				engine.makeQueryStepsDag(executingQueryContext));

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getAggregators()).hasSize(2).contains(k1Sum, k2Sum);
		});
	}
}
