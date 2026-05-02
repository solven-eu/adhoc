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
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.grouper.TableStepsGrouperByAffinity;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestTableStepsGrouperByAffinity {

	TableStepsGrouperByAffinity grouper = new TableStepsGrouperByAffinity();

	TableQueryStep step_m1_col1 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m1")).groupBy(GroupByColumns.named("col1")).build();

	TableQueryStep step_m1_col2 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m1")).groupBy(GroupByColumns.named("col2")).build();

	TableQueryStep step_m2_col1 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m2")).groupBy(GroupByColumns.named("col1")).build();

	TableQueryStep step_m2_col2 =
			TableQueryStep.builder().aggregator(Aggregator.sum("m2")).groupBy(GroupByColumns.named("col2")).build();

	@Test
	public void testSingleStep() {
		Collection<? extends Collection<TableQueryStep>> groups = grouper.groupInducers(ImmutableSet.of(step_m1_col1));

		Assertions.assertThat(groups).hasSize(1);
		Assertions.assertThat(groups.iterator().next()).containsExactly(step_m1_col1);
	}

	@Test
	public void testSameMeasure_multipleGroupBys_oneGroup() {
		// m1 at col1 and m1 at col2 → single biclique {m1} × {col1, col2}
		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1, step_m1_col2));

		Assertions.assertThat(groups).hasSize(1);
		Assertions.assertThat(groups.iterator().next()).containsExactlyInAnyOrder(step_m1_col1, step_m1_col2);
	}

	@Test
	public void testSameGroupBy_multipleMeasures_oneGroup() {
		// m1 at col1 and m2 at col1 → single biclique {m1, m2} × {col1}
		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1, step_m2_col1));

		Assertions.assertThat(groups).hasSize(1);
		Assertions.assertThat(groups.iterator().next()).containsExactlyInAnyOrder(step_m1_col1, step_m2_col1);
	}

	@Test
	public void testFullBiclique_oneGroup() {
		// m1×col1, m1×col2, m2×col1, m2×col2 → full biclique {m1,m2} × {col1,col2}
		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1, step_m1_col2, step_m2_col1, step_m2_col2));

		Assertions.assertThat(groups).hasSize(1);
		Assertions.assertThat(groups.iterator().next())
				.containsExactlyInAnyOrder(step_m1_col1, step_m1_col2, step_m2_col1, step_m2_col2);
	}

	@Test
	public void testSparseGraph_splitIntoTwoGroups() {
		// m1×col1, m1×col2, m2×col1 — m2 does NOT need col2
		// Expected bicliques: {m1} × {col1, col2} and {m2} × {col1}
		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1, step_m1_col2, step_m2_col1));

		Assertions.assertThat(groups).hasSize(2);

		long groupsContainingM1col2 = groups.stream().filter(g -> g.contains(step_m1_col2)).count();
		Assertions.assertThat(groupsContainingM1col2).as("step_m1_col2 must appear in exactly one group").isEqualTo(1);

		// step_m1_col1 and step_m1_col2 must be in the same group (both use m1)
		Collection<TableQueryStep> m1Group = groups.stream().filter(g -> g.contains(step_m1_col2)).findFirst().get();
		Assertions.assertThat(m1Group).contains(step_m1_col1);
		Assertions.assertThat(m1Group).doesNotContain(step_m2_col1);

		// step_m2_col1 must be alone in its group
		Collection<TableQueryStep> m2Group = groups.stream().filter(g -> g.contains(step_m2_col1)).findFirst().get();
		Assertions.assertThat(m2Group).containsExactly(step_m2_col1);
	}

	@Test
	public void testFullyDisjoint_twoGroups() {
		// m1×col1, m2×col2 — no shared measure or groupBy
		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1, step_m2_col2));

		Assertions.assertThat(groups).hasSize(2);
		Set<TableQueryStep> allSteps =
				groups.stream().flatMap(Collection::stream).collect(ImmutableSet.toImmutableSet());
		Assertions.assertThat(allSteps).containsExactlyInAnyOrder(step_m1_col1, step_m2_col2);
	}

	@Test
	public void testFilterDifferentiatesLeftKey() {
		// Same measure, same groupBy, different filter → different left keys → 2 groups
		TableQueryStep step_m1_col1_eur = TableQueryStep.builder()
				.aggregator(Aggregator.sum("m1"))
				.groupBy(GroupByColumns.named("col1"))
				.filter(ColumnFilter.matchEq("ccy", "EUR"))
				.build();
		TableQueryStep step_m1_col2_usd = TableQueryStep.builder()
				.aggregator(Aggregator.sum("m1"))
				.groupBy(GroupByColumns.named("col2"))
				.filter(ColumnFilter.matchEq("ccy", "USD"))
				.build();

		Collection<? extends Collection<TableQueryStep>> groups =
				grouper.groupInducers(ImmutableSet.of(step_m1_col1_eur, step_m1_col2_usd));

		// Different filters mean different left keys; neither covers the other's groupBy
		Assertions.assertThat(groups).hasSize(2);
	}

}
