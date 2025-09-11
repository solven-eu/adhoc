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

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer.SplitTableQueries;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;

public class TestTableQueryOptimizerSinglePerAggregator implements IAdhocTestConstants {

	CubeQueryStep step = CubeQueryStep.builder().measure(k1Sum).build();

	TableQueryOptimizerSinglePerAggregator optimizer =
			new TableQueryOptimizerSinglePerAggregator(AdhocFactories.builder().build());

	@Test
	public void testCanInduce_OrDifferentColumns() {
		TableQuery tq1 = TableQuery.edit(step)
				.filter(ColumnFilter.isEqualTo("a", "a1"))
				.groupBy(GroupByColumns.named("b"))
				.aggregator(k1Sum)
				.build();
		TableQuery tq2 = TableQuery.edit(step)
				.filter(ColumnFilter.isEqualTo("c", "c1"))
				.groupBy(GroupByColumns.named("d"))
				.aggregator(k1Sum)
				.build();
		SplitTableQueries split = optimizer.splitInduced(() -> Set.of(), Set.of(tq1, tq2));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(CubeQueryStep.edit(step)
						.filter(FilterBuilder.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("c", "c1"))
								.optimize())
						.groupBy(GroupByColumns.named("a", "b", "c", "d"))
						.build());

		Assertions.assertThat(split.getInduceds())
				.hasSize(2)
				.contains(CubeQueryStep.edit(step)
						.filter(ColumnFilter.isEqualTo("a", "a1"))
						.groupBy(GroupByColumns.named("b"))
						.build())
				.contains(CubeQueryStep.edit(step)
						.filter(ColumnFilter.isEqualTo("c", "c1"))
						.groupBy(GroupByColumns.named("d"))
						.build());
	}

	@Test
	public void testCanInduce_OrDifferentColumns_noMeasure() {
		TableQuery tq1 = TableQuery.edit(step)
				.filter(ColumnFilter.isEqualTo("a", "a1"))
				.groupBy(GroupByColumns.named("b"))
				.build();
		TableQuery tq2 = TableQuery.edit(step)
				.filter(ColumnFilter.isEqualTo("c", "c1"))
				.groupBy(GroupByColumns.named("d"))
				.build();
		SplitTableQueries split = optimizer.splitInduced(() -> Set.of(), Set.of(tq1, tq2));

		Assertions.assertThat(split.getInducers())
				.hasSize(1)
				.contains(CubeQueryStep.edit(step)
						.filter(FilterBuilder.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("c", "c1"))
								.optimize())
						.groupBy(GroupByColumns.named("a", "b", "c", "d"))
						.measure(Aggregator.empty())
						.build());

		Assertions.assertThat(split.getInduceds())
				.hasSize(2)
				.contains(CubeQueryStep.edit(step)
						.filter(ColumnFilter.isEqualTo("a", "a1"))
						.groupBy(GroupByColumns.named("b"))
						.measure(Aggregator.empty())
						.build())
				.contains(CubeQueryStep.edit(step)
						.filter(ColumnFilter.isEqualTo("c", "c1"))
						.groupBy(GroupByColumns.named("d"))
						.measure(Aggregator.empty())
						.build());
	}
}
