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
package eu.solven.adhoc.engine.tabular.optimizer.merger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.IAdhocDag;
import eu.solven.adhoc.engine.tabular.splitter.merger.IMergeInducers;
import eu.solven.adhoc.engine.tabular.splitter.merger.MergeInducersStrictGroupBy;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestMergeInducersStrictGroupBy {
	IMergeInducers merger = MergeInducersStrictGroupBy.builder().build();

	TableQueryStep a = TableQueryStep.builder().aggregator(Aggregator.sum("k")).build();

	@Test
	public void noGroupBy_filterSameColumn() {
		TableQueryStep s1 = a.toBuilder().filter(ColumnFilter.matchEq("a", "a1")).build();
		TableQueryStep s2 = a.toBuilder().filter(ColumnFilter.matchEq("a", "a2")).build();

		IAdhocDag<TableQueryStep> merged = merger.mergeInducers(a, ImmutableSet.of(s1, s2));

		Assertions.assertThat(merged.vertexSet()).hasSize(0);
	}

	@Test
	public void someGroupBy_filterGroupedBy() {
		TableQueryStep s1 =
				a.toBuilder().groupBy(GroupByColumns.named("a")).filter(ColumnFilter.matchEq("a", "a1")).build();
		TableQueryStep s2 =
				a.toBuilder().groupBy(GroupByColumns.named("a")).filter(ColumnFilter.matchEq("a", "a2")).build();

		IAdhocDag<TableQueryStep> merged = merger.mergeInducers(a, ImmutableSet.of(s1, s2));

		Assertions.assertThat(merged.vertexSet())
				.hasSize(3)
				.contains(s1, s2)
				.contains(a.toBuilder()
						.filter(ColumnFilter.matchIn("a", "a1", "a2"))
						.groupBy(GroupByColumns.named("a"))
						.build());

		Assertions.assertThat(merged.edgeSet()).hasSize(2);
	}

	@Test
	public void differentGroupBy_filterSameColumn() {
		TableQueryStep s1 =
				a.toBuilder().groupBy(GroupByColumns.named("a")).filter(ColumnFilter.matchEq("b", "b1")).build();
		TableQueryStep s2 =
				a.toBuilder().groupBy(GroupByColumns.named("c")).filter(ColumnFilter.matchEq("d", "d1")).build();

		IAdhocDag<TableQueryStep> merged = merger.mergeInducers(a, ImmutableSet.of(s1, s2));

		Assertions.assertThat(merged.vertexSet()).hasSize(0);
	}

	@Test
	public void differentGroupBy_sameFilter() {
		TableQueryStep s1 =
				a.toBuilder().groupBy(GroupByColumns.named("a")).filter(ColumnFilter.matchEq("b", "b1")).build();
		TableQueryStep s2 =
				a.toBuilder().groupBy(GroupByColumns.named("c")).filter(ColumnFilter.matchEq("b", "b1")).build();

		IAdhocDag<TableQueryStep> merged = merger.mergeInducers(a, ImmutableSet.of(s1, s2));

		Assertions.assertThat(merged.vertexSet()).hasSize(0);
	}

	@Test
	public void fineGroupByWithFilter_coarseGroupByWithoutFilter() {
		TableQueryStep s1 =
				a.toBuilder().groupBy(GroupByColumns.named("a", "b")).filter(ColumnFilter.matchEq("a", "a1")).build();
		TableQueryStep s2 = a.toBuilder().groupBy(GroupByColumns.named("a")).build();

		IAdhocDag<TableQueryStep> merged = merger.mergeInducers(a, ImmutableSet.of(s1, s2));

		Assertions.assertThat(merged.vertexSet()).hasSize(0);
	}
}
