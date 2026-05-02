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
package eu.solven.adhoc.engine.tabular.splitter;

import java.util.Collection;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.grouper.TableStepsGrouperByAggregator;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Demonstrates how to keep in a dedicated tqbleQuery all TableQuerySteps referring to a column.
 */
public class TestTableStepsGrouperByAggregator_highCardinality {
	String highC = "highC";

	TableStepsGrouperByAggregator grouper = new TableStepsGrouperByAggregator() {
		@Override
		protected TableQueryStep contextOnly(TableQueryStep inducer) {
			TableQueryStep superGroup = super.contextOnly(inducer);

			if (TableQueryStep.getColumns(inducer).contains(highC)) {
				// By keeping a subset of groupedBy columns, we force these querySteps aside
				return superGroup.toBuilder().groupBy(GroupByColumns.named(highC)).build();
			} else {
				return superGroup;
			}
		}
	};

	// measure m1, two different groupBys
	TableQueryStep step_m1_col1 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m1")).groupBy(GroupByColumns.named("col1")).build();
	TableQueryStep step_m1_col2 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m1")).groupBy(GroupByColumns.named("col2")).build();
	TableQueryStep step_m1_highC =
			TableQueryStep.builder().aggregator(Aggregator.sum("m1")).groupBy(GroupByColumns.named("highC")).build();

	// measure m2
	TableQueryStep step_m2_col1 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m2")).groupBy(GroupByColumns.named("col1")).build();

	// Nominal: 3 steps over 2 distinct measures → 2 TableQuery groups.
	// step_m1_col1 and step_m1_col2 share the same measure (m1) so they collapse into one group;
	// step_m2_col1 has a different measure (m2) so it forms its own group.
	@Test
	public void testThreeStepsTwoMeasures_twoGroups() {
		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1, step_m1_col2, step_m2_col1, step_m1_highC));

		Assertions.assertThat(groups).hasSize(3);

		Collection<TableQueryStep> m1Group =
				groups.stream().filter(g -> g.contains(step_m1_col1)).findFirst().orElseThrow();
		Assertions.assertThat(m1Group).containsExactlyInAnyOrder(step_m1_col1, step_m1_col2);

		Collection<TableQueryStep> m2Group =
				groups.stream().filter(g -> g.contains(step_m2_col1)).findFirst().orElseThrow();
		Assertions.assertThat(m2Group).containsExactly(step_m2_col1);

		Collection<TableQueryStep> highCGroup =
				groups.stream().filter(g -> g.contains(step_m1_highC)).findFirst().orElseThrow();
		Assertions.assertThat(highCGroup).containsExactly(step_m1_highC);
	}
}
