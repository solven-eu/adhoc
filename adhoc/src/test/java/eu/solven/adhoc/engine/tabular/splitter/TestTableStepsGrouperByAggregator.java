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

public class TestTableStepsGrouperByAggregator {

	TableStepsGrouperByAggregator grouper = new TableStepsGrouperByAggregator();

	// measure m1, two different groupBys
	TableQueryStep step_m1_col1 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m1")).groupBy(GroupByColumns.named("col1")).build();
	TableQueryStep step_m1_col2 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m1")).groupBy(GroupByColumns.named("col2")).build();

	// measure m2
	TableQueryStep step_m2_col1 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m2")).groupBy(GroupByColumns.named("col1")).build();

	// Nominal: 3 steps over 2 distinct measures → 2 TableQuery groups.
	// step_m1_col1 and step_m1_col2 share the same measure (m1) so they collapse into one group;
	// step_m2_col1 has a different measure (m2) so it forms its own group.
	@Test
	public void testThreeStepsTwoMeasures_twoGroups() {
		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1, step_m1_col2, step_m2_col1));

		Assertions.assertThat(groups).hasSize(2);

		Collection<TableQueryStep> m1Group =
				groups.stream().filter(g -> g.contains(step_m1_col1)).findFirst().orElseThrow();
		Assertions.assertThat(m1Group).containsExactlyInAnyOrder(step_m1_col1, step_m1_col2);

		Collection<TableQueryStep> m2Group =
				groups.stream().filter(g -> g.contains(step_m2_col1)).findFirst().orElseThrow();
		Assertions.assertThat(m2Group).containsExactly(step_m2_col1);
	}

	// A single step produces exactly one group containing that step.
	@Test
	public void testSingleStep_oneGroup() {
		Collection<? extends Collection<TableQueryStep>> groups = grouper.groupInducers(ImmutableSet.of(step_m1_col1));

		Assertions.assertThat(groups).hasSize(1);
		Assertions.assertThat(groups.iterator().next()).containsExactly(step_m1_col1);
	}

	// Two steps with distinct measures each go into their own group.
	@Test
	public void testTwoDistinctMeasures_twoGroups() {
		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1, step_m2_col1));

		Assertions.assertThat(groups).hasSize(2);
	}

	// Two steps sharing the same measure collapse into a single group regardless of groupBy.
	@Test
	public void testSameMeasureDifferentGroupBys_oneGroup() {
		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1, step_m1_col2));

		Assertions.assertThat(groups).hasSize(1);
		Assertions.assertThat(groups.iterator().next()).containsExactlyInAnyOrder(step_m1_col1, step_m1_col2);
	}
}
