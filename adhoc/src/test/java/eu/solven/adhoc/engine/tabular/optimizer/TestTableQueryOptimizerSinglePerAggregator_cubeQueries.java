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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.eventbus.Subscribe;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.eventbus.TableStepIsEvaluating;
import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * This will test actual queries over TableQueryOptimizerSinglePerAggregator.
 */
public class TestTableQueryOptimizerSinglePerAggregator_cubeQueries extends ADagTest {

	TableQueryOptimizerSinglePerAggregator optimizer =
			new TableQueryOptimizerSinglePerAggregator(AdhocFactories.builder().build());

	@BeforeEach
	@Override
	public void feedTable() throws Exception {
		table().add(Map.of("a", "a1", "b", "b1", "k1", 123));
		table().add(Map.of("a", "a1", "b", "b2", "k1", 345));
		table().add(Map.of("a", "a2", "b", "b1", "k1", 567));

		forest.addMeasure(Aggregator.sum("k1"));

		forest.addMeasure(Partitionor.builder()
				.name("k1Sum_maxByA")
				.aggregationKey(MaxCombination.KEY)
				.groupBy(GroupByColumns.named("a"))
				.underlying("k1")
				.build());

		forest.addMeasure(Partitionor.builder()
				.name("k1Sum_maxByB")
				.aggregationKey(MaxCombination.KEY)
				.groupBy(GroupByColumns.named("b"))
				.underlying("k1")
				.build());
	}

	// Given 2 measures doing a groupBy on different columns, we do a single tableQuery with both groupBy
	@Test
	public void testCanInduce_groupByDifferentColumns() {

		ICubeWrapper cube = CubeWrapperEditor.edit(cube()).editTableQueryOptimizer(new TableQueryOptimizerFactory() {
			@Override
			protected ITableQueryOptimizer makeOptimizer(AdhocFactories factories) {
				return new TableQueryOptimizerSinglePerAggregator(factories);
			}
		}).build();

		List<TableStepIsEvaluating> tableEvaluating = new ArrayList<>();

		eventBus.register(new Object() {
			@Subscribe
			public void onEvent(TableStepIsEvaluating o) {
				tableEvaluating.add(o);
			}
		});

		ITabularView output =
				cube.execute(CubeQuery.builder().measure("k1Sum_maxByA", "k1Sum_maxByB").explain(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		// Checks the figures
		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v)
					// a2 as a1=123+345
					.containsEntry("k1Sum_maxByA", 0L + 567)
					// b1 as b2 =456
					.containsEntry("k1Sum_maxByB", 0L + 123 + 567)
					.hasSize(2);
		});

		// This ensures we made a single tableQuery with both groupBy
		Assertions.assertThat(tableEvaluating).hasSize(1).anySatisfy(e -> {
			Assertions.assertThat(e.getTableQuery().getAggregators()).hasSize(1).anySatisfy(fa -> {
				Assertions.assertThat(fa.getAggregator().getName()).isEqualTo("k1");
			});
			Assertions.assertThat(e.getTableQuery().getGroupBy().getGroupedByColumns()).containsExactly("a", "b");
		});
	}

	@Test
	public void testCanInduce_groupByDifferentColumns_differentFilters() {
		forest.addMeasure(Aggregator.sum("k1"));

		forest.addMeasure(Filtrator.builder()
				.name("k1Sum_maxByA_b1")
				.underlying("k1Sum_maxByA")
				.filter(ColumnFilter.isEqualTo("b", "b1"))
				.build());

		forest.addMeasure(Filtrator.builder()
				.name("k1Sum_maxByB_a1")
				.underlying("k1Sum_maxByB")
				.filter(ColumnFilter.isEqualTo("a", "a1"))
				.build());

		ICubeWrapper cube = CubeWrapperEditor.edit(cube()).editTableQueryOptimizer(new TableQueryOptimizerFactory() {
			@Override
			protected ITableQueryOptimizer makeOptimizer(AdhocFactories factories) {
				return new TableQueryOptimizerSinglePerAggregator(factories);
			}
		}).build();

		List<TableStepIsEvaluating> tableEvaluating = new ArrayList<>();

		eventBus.register(new Object() {
			@Subscribe
			public void onEvent(TableStepIsEvaluating o) {
				tableEvaluating.add(o);
			}
		});

		ITabularView output =
				cube.execute(CubeQuery.builder().measure("k1Sum_maxByA_b1", "k1Sum_maxByB_a1").explain(true).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		// Checks the figures
		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v)
					// a2 as a1=123+345
					.containsEntry("k1Sum_maxByA_b1", 0L + 567)
					// b1 as b2 =456.
					.containsEntry("k1Sum_maxByB_a1", 0L + 345)
					.hasSize(2);
		});

		// This ensures we made a single tableQuery with both groupBy
		Assertions.assertThat(tableEvaluating).hasSize(1).anySatisfy(e -> {
			Assertions.assertThat(e.getTableQuery().getAggregators()).hasSize(1).anySatisfy(fa -> {
				Assertions.assertThat(fa.getAggregator().getName()).isEqualTo("k1");
				Assertions.assertThat(fa.getFilter()).isEqualTo(ISliceFilter.MATCH_ALL);
			});
			Assertions.assertThat(e.getTableQuery().getGroupBy().getGroupedByColumns()).containsExactly("a", "b");
			Assertions.assertThat(e.getTableQuery().getFilter()).isEqualTo(OrFilter.or(Map.of("a", "a1", "b", "b1")));
		});
	}
}
