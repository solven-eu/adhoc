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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.engine.MapQueryStepCache;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.IHasQueryOptions;

public class TestPrepareTableQuery extends ADagTest implements IAdhocTestConstants {

	TableQueryEngine engine = (TableQueryEngine) engine().getTableQueryEngine();
	ITableQueryOptimizer optimizer =
			engine.optimizerFactory.makeOptimizer(engine.getFactories(), IHasQueryOptions.noOption());
	TableQueryEngineBootstrapped bootstrapped = engine.bootstrap(optimizer);

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

		QueryPod queryPod = QueryPod.builder()
				.query(CubeQuery.builder().measure(k1Sum).build())
				.forest(forest)
				.table(table())
				.build();
		Set<CubeQueryStep> output = bootstrapped.prepareForTable(queryPod, engine().makeQueryStepsDag(queryPod));

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getMeasure()).isEqualTo(k1Sum);
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

		QueryPod queryPod = QueryPod.builder()
				.query(CubeQuery.builder().measure("sumK1K2").build())
				.forest(forest)
				.table(table())
				.build();
		Set<CubeQueryStep> output = bootstrapped.prepareForTable(queryPod, engine().makeQueryStepsDag(queryPod));

		Assertions.assertThat(output).hasSize(2).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getMeasure()).isEqualTo(k1Sum);
		}).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getMeasure()).isEqualTo(k2Sum);
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

		QueryPod queryPod = QueryPod.builder()
				.query(CubeQuery.builder().measure("sumK1K2").build())
				.forest(forest)
				.table(table())
				.build();
		Set<CubeQueryStep> output = bootstrapped.prepareForTable(queryPod, engine().makeQueryStepsDag(queryPod));

		Assertions.assertThat(output).hasSize(2).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getMeasure()).isEqualTo(k1Sum);
		}).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			Assertions.assertThat(dbQuery.getMeasure()).isEqualTo(k2Sum);
		});
	}

	@Test
	public void testSum_inCache() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);
		forest.addMeasure(k1PlusK2AsExpr);

		MapQueryStepCache queryStepCache = new MapQueryStepCache();
		queryStepCache.pushValues(Map.of(CubeQueryStep.builder().measure(k1Sum).build(), SliceToValue.empty()));

		QueryPod queryPod = QueryPod.builder()
				.query(CubeQuery.builder().measure(k1PlusK2AsExpr).build())
				.forest(forest)
				.table(table())
				.queryStepCache(queryStepCache)
				.build();
		Set<CubeQueryStep> output = bootstrapped.prepareForTable(queryPod, engine().makeQueryStepsDag(queryPod));

		Assertions.assertThat(output).hasSize(1).anySatisfy(dbQuery -> {
			Assertions.assertThat(dbQuery.getFilter().isMatchAll()).isTrue();
			Assertions.assertThat(dbQuery.getGroupBy().isGrandTotal()).isTrue();

			// query only k2Sum as k1Sum is in cache
			Assertions.assertThat(dbQuery.getMeasure()).isEqualTo(k2Sum);
		});
	}
}
