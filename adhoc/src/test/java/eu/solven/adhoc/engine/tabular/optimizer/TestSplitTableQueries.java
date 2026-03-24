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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.IHasTableQueryForSteps.StepAndFilteredAggregator;
import eu.solven.adhoc.filter.optimizer.FilterOptimizer;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;

public class TestSplitTableQueries {

	FilterOptimizer filterOptimizer = FilterOptimizer.builder().build();

	Aggregator sumM1 = Aggregator.sum("m1");
	FilteredAggregator fa = FilteredAggregator.builder().aggregator(sumM1).build();
	TableQueryV4 query = TableQueryV4.builder().groupByToAggregator(GroupByColumns.named("a"), fa).build();

	// -------------------------------------------------------------------------
	// forEachCubeQuerySteps — step absent from stepToTables
	// -------------------------------------------------------------------------

	@Test
	public void testForEachCubeQuerySteps_stepNotInMap_returnsEmpty() {
		// SplitTableQueries.empty() has an empty stepToTables, so containsStep always returns false.
		SplitTableQueries split = SplitTableQueries.empty();

		List<StepAndFilteredAggregator> result = split.forEachCubeQuerySteps(query, filterOptimizer).toList();

		// The step produced by recombineQueryStep is not in stepToTables → must be filtered out, not returned as null.
		Assertions.assertThat(result).isEmpty();
	}

	// -------------------------------------------------------------------------
	// forEachCubeQuerySteps — step present in stepToTables
	// -------------------------------------------------------------------------

	@Test
	public void testForEachCubeQuerySteps_stepInMap_returnsStep() {
		TableQueryStep step = query.recombineQueryStep(filterOptimizer, fa, GroupByColumns.named("a"));
		TableQueryV4 tableQueryV4 = TableQueryV4.builder().groupByToAggregator(GroupByColumns.named("a"), fa).build();

		SplitTableQueries split = SplitTableQueries.builder()
				.inducedToInducer(GraphHelpers.makeGraph())
				.stepToTable(step, tableQueryV4)
				.build();

		List<StepAndFilteredAggregator> result = split.forEachCubeQuerySteps(query, filterOptimizer).toList();

		Assertions.assertThat(result).hasSize(1);
		Assertions.assertThat(result.getFirst().step()).isEqualTo(step);
		Assertions.assertThat(result.getFirst().aggregator()).isEqualTo(fa);
	}
}
