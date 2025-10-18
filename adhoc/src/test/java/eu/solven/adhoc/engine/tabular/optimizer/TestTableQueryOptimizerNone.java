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

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer.SplitTableQueries;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.optimizer.FilterOptimizer;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;

public class TestTableQueryOptimizerNone {
	CubeQueryStep step = CubeQueryStep.builder()
			.measure("m1")
			.groupBy(GroupByColumns.named("g", "h"))
			.filter(ColumnFilter.matchEq("c", "c1"))
			.build();

	TableQueryOptimizerNone optimizer =
			new TableQueryOptimizerNone(AdhocFactories.builder().build(), FilterOptimizer.builder().build());

	@Test
	public void testCanInduce() {
		TableQuery tq1 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("a", "a1"))
				.groupBy(GroupByColumns.named("b"))
				.aggregator(Aggregator.sum("m1"))
				.build();
		TableQuery tq2 = TableQuery.edit(step)
				.filter(ColumnFilter.matchEq("c", "c1"))
				.groupBy(GroupByColumns.named("d"))
				.aggregator(Aggregator.sum("m1"))
				.build();
		SplitTableQueries split = optimizer.splitInduced(() -> Set.of(), Set.of(tq1, tq2));

		Assertions.assertThat(split.getInducers())
				.hasSize(2)
				.contains(CubeQueryStep.edit(tq1).measure(Aggregator.sum("m1")).build())
				.contains(CubeQueryStep.edit(tq2).measure(Aggregator.sum("m1")).build());
	}
}
