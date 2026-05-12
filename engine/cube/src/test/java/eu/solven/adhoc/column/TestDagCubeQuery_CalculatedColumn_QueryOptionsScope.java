/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.column;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.calculated.ICalculatedColumn;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.factories.QueryOptionsScope;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.column.FunctionCalculatedColumn;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.options.StandardQueryOptions;

/**
 * End-to-end test that an {@link ICalculatedColumn} can read the executing query's options via
 * {@link QueryOptionsScope#current()} (or {@link QueryOptionsScope#isActive(eu.solven.adhoc.options.IQueryOption)})
 * during {@code computeCoordinate}. The motivating use case for {@link QueryOptionsScope} is a complex calculated
 * column that does a DB round-trip — it needs to know whether {@code NO_CACHE} or {@code EXPLAIN} is active without
 * having the option set threaded through its API.
 *
 * @author Benoit Lacelle
 */
public class TestDagCubeQuery_CalculatedColumn_QueryOptionsScope extends ATestDagInMemory
		implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 100));
		table().add(Map.of("a", "a2", "k1", 200));

		forest.addMeasure(Aggregator.builder().name("k1").columnName("k1").aggregationKey(SumAggregation.KEY).build());
	}

	// Calculated column whose output reflects whether NO_CACHE is active. A real-world complex column would use the
	// flag to decide whether to consult a cache; here we just label the row with the flag's state so the test can
	// assert it.
	protected ICalculatedColumn noCacheAwareColumn() {
		return FunctionCalculatedColumn.builder()
				.name("scope_options")
				.recordToCoordinate(
						r -> QueryOptionsScope.isActive(StandardQueryOptions.NO_CACHE) ? "no_cache" : "cached")
				.build();
	}

	@Test
	public void testCalculatedColumn_readsActiveOption_NO_CACHE() {
		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1")
				.groupByAlso(noCacheAwareColumn())
				.option(StandardQueryOptions.NO_CACHE)
				.build());

		// NO_CACHE in the query options -> column emits "no_cache" for every row.
		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("scope_options", "no_cache"), Map.of("k1", 0L + 100 + 200))
				.hasSize(1);
	}

	@Test
	public void testCalculatedColumn_readsActiveOption_default_noCacheInactive() {
		ITabularView view = cube().execute(CubeQuery.builder().measure("k1").groupByAlso(noCacheAwareColumn()).build());

		// Default options -> NO_CACHE inactive -> column emits "cached".
		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("scope_options", "cached"), Map.of("k1", 0L + 100 + 200))
				.hasSize(1);
	}

	@Test
	public void testCalculatedColumn_readsActiveOption_otherOption_doesNotActivateNoCache() {
		// EXPLAIN is active but NO_CACHE is NOT — the column should read "cached". Pins the per-option isActive
		// check rather than "any option present".
		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1")
				.groupByAlso(noCacheAwareColumn())
				.option(StandardQueryOptions.EXPLAIN)
				.build());

		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("scope_options", "cached"), Map.of("k1", 0L + 100 + 200))
				.hasSize(1);
	}

	@Test
	public void testQueryOptionsScope_isUnboundOutsideEngineExecute() {
		// Outside any CubeQueryEngine#execute call, no scope is active — the helper degrades gracefully.
		Assertions.assertThat(QueryOptionsScope.current()).isEmpty();
		Assertions.assertThat(QueryOptionsScope.isBound()).isFalse();
		Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.NO_CACHE)).isFalse();
	}
}
