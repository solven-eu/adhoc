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

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;

public class TestTableQueryV4Merger {

	private final IFilterOptimizer filterOptimizer =
			AdhocFactories.builder().build().getFilterOptimizerFactory().makeOptimizer();

	private final Aggregator k1 = Aggregator.sum("k1");
	private final Aggregator k2 = Aggregator.sum("k2");

	@Test
	public void testMerge_empty_throws() {
		Assertions.assertThatThrownBy(() -> TableQueryV4Merger.merge(Set.of(), filterOptimizer))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("empty");
	}

	@Test
	public void testMerge_singleQuery_returnsAsIs() {
		TableQueryV4 only = TableQueryV4.builder()
				.filter(ColumnFilter.matchEq("c", "c1"))
				.groupByToAggregator(GroupByColumns.named("a"), FilteredAggregator.builder().aggregator(k1).build())
				.build();

		TableQueryV4 merged = TableQueryV4Merger.merge(Set.of(only), filterOptimizer);

		Assertions.assertThat(merged).isSameAs(only);
	}

	@Test
	public void testMerge_disjointGroupBys_unionsColumns() {
		TableQueryV4 q1 = TableQueryV4.builder()
				.filter(ColumnFilter.matchEq("c", "c1"))
				.groupByToAggregator(GroupByColumns.named("a"), FilteredAggregator.builder().aggregator(k1).build())
				.build();
		TableQueryV4 q2 = TableQueryV4.builder()
				.filter(ColumnFilter.matchEq("c", "c2"))
				.groupByToAggregator(GroupByColumns.named("b"), FilteredAggregator.builder().aggregator(k2).build())
				.build();

		TableQueryV4 merged = TableQueryV4Merger.merge(ImmutableSet.of(q1, q2), filterOptimizer);

		Assertions.assertThat(merged.getGroupBys()).hasSize(1);
		IGroupBy mergedGroupBy = merged.getGroupBys().iterator().next();
		Assertions.assertThat(mergedGroupBy.getSortedColumns()).containsExactlyInAnyOrder("a", "b");
		Assertions.assertThat(merged.getAggregators()).hasSize(2);
	}

	@Test
	public void testMerge_filterIsOrCombined() {
		ISliceFilter f1 = ColumnFilter.matchEq("c", "c1");
		ISliceFilter f2 = ColumnFilter.matchEq("c", "c2");
		TableQueryV4 q1 = TableQueryV4.builder()
				.filter(f1)
				.groupByToAggregator(GroupByColumns.named("a"), FilteredAggregator.builder().aggregator(k1).build())
				.build();
		TableQueryV4 q2 = TableQueryV4.builder()
				.filter(f2)
				.groupByToAggregator(GroupByColumns.named("a"), FilteredAggregator.builder().aggregator(k2).build())
				.build();

		TableQueryV4 merged = TableQueryV4Merger.merge(ImmutableSet.of(q1, q2), filterOptimizer);

		// The merged WHERE filter accepts rows matching either input — i.e. both c=c1 and c=c2 pass.
		Assertions.assertThat(merged.getFilter()).isNotEqualTo(f1).isNotEqualTo(f2);
	}

	@Test
	public void testMerge_sameAggregator_reindexesAlias() {
		// Same Aggregator name k1, but distinct per-aggregator FILTERs: aliases must be made unique.
		TableQueryV4 q1 = TableQueryV4.builder()
				.groupByToAggregator(IGroupBy.GRAND_TOTAL,
						FilteredAggregator.builder().aggregator(k1).filter(ColumnFilter.matchEq("c", "c1")).build())
				.build();
		TableQueryV4 q2 = TableQueryV4.builder()
				.groupByToAggregator(IGroupBy.GRAND_TOTAL,
						FilteredAggregator.builder().aggregator(k1).filter(ColumnFilter.matchEq("c", "c2")).build())
				.build();

		TableQueryV4 merged = TableQueryV4Merger.merge(ImmutableSet.of(q1, q2), filterOptimizer);

		Assertions.assertThat(merged.getAggregators()).hasSize(2);
		Assertions.assertThat(merged.getAggregators().stream().map(FilteredAggregator::getAlias))
				.containsExactlyInAnyOrder("k1", "k1_1");
	}

	@Test
	public void testMerge_identicalAggregator_dedupes() {
		// Same Aggregator k1, same FILTER (matchAll): the duplicate must collapse into a single output aggregator.
		TableQueryV4 q1 = TableQueryV4.builder()
				.groupByToAggregator(GroupByColumns.named("a"), FilteredAggregator.builder().aggregator(k1).build())
				.build();
		TableQueryV4 q2 = TableQueryV4.builder()
				.groupByToAggregator(GroupByColumns.named("a"), FilteredAggregator.builder().aggregator(k1).build())
				.build();

		TableQueryV4 merged = TableQueryV4Merger.merge(ImmutableSet.of(q1, q2), filterOptimizer);

		Assertions.assertThat(merged.getAggregators()).hasSize(1);
		Assertions.assertThat(merged.getAggregators().iterator().next().getAlias()).isEqualTo("k1");
	}

	@Test
	public void testMergeForDrillthrough_widensWhereFromPerAggregatorFilters() {
		// Regression test: the input V4 has a permissive WHERE (matchAll) but two aggregators each carry their
		// own per-aggregator FILTER. The merged WHERE must NOT degrade to MATCH_ALL — it must widen to
		// `OR(a=a1, a=a2)` so row inclusion is preserved. Without this, every DB row would survive instead of
		// just those matching either FILTER.
		FilteredAggregator agg1 =
				FilteredAggregator.builder().aggregator(k1).filter(ColumnFilter.matchEq("a", "a1")).build();
		FilteredAggregator agg2 =
				FilteredAggregator.builder().aggregator(k2).filter(ColumnFilter.matchEq("a", "a2")).build();
		TableQueryV4 input = TableQueryV4.builder()
				.filter(ISliceFilter.MATCH_ALL)
				.groupByToAggregators(IGroupBy.GRAND_TOTAL, ImmutableSet.of(agg1, agg2))
				.build();

		TableQueryV3 merged = TableQueryV4Merger.mergeForDrillthrough(ImmutableSet.of(input), filterOptimizer);

		Assertions.assertThat(merged.getFilter()).isNotEqualTo(ISliceFilter.MATCH_ALL);
		// Per-aggregator FILTERs are preserved verbatim (the row-streaming layer applies them per-row to
		// populate the alias column with null when the FILTER does not match). Row inclusion comes from the
		// widened merged WHERE, not from the per-aggregator FILTERs.
		Assertions.assertThat(merged.getAggregators())
				.extracting(fa -> fa.getFilter())
				.containsExactlyInAnyOrder(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("a", "a2"));
		// The widened WHERE references the original FILTERs' columns, so `addFilteredColumnsToGroupBy` surfaces `a`.
		Assertions.assertThat(merged.getGroupBys().iterator().next().getSortedColumns()).contains("a");
	}
}
