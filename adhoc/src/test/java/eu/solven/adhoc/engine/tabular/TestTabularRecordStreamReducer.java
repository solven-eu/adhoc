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
package eu.solven.adhoc.engine.tabular;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.dataframe.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.tabular.inducer.ITableQueryInducer;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryFactory;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.table.InMemoryTable;

public class TestTabularRecordStreamReducer implements IAdhocTestConstants {
	TableQueryEngineFactory engine = TableQueryEngineFactory.builder().build();

	ITableQueryInducer inducer = engine.inducerFactory.makeInducer(engine.getFactories());
	ITableQueryFactory optimizer =
			engine.queryFactoryFactory.makeQueryFactory(engine.getFactories(), IHasQueryOptions.noOption());

	@Test
	public void testDistinct() {
		InMemoryTable tableWrapper = InMemoryTable.builder().distinctSlices(true).build();
		TableQueryV3 tableQuery =
				TableQueryV3.edit(TableQuery.builder().aggregator(k1Sum).aggregator(k2Sum).build()).build();

		// InMemoryTable will produce a single row with both aggregates
		tableWrapper.add(Map.of("k1", 123, "k2", 123));

		ITabularRecordStream stream = tableWrapper.streamSlices(tableQuery);

		TableQueryEngine bootstrapped =
				(TableQueryEngine) engine.bootstrap(QueryPod.forTable(tableWrapper), optimizer, inducer);
		IMultitypeMergeableGrid<ISlice> merged = bootstrapped.mergeTableAggregates(tableQuery, stream);

		Assertions.assertThat(merged.size(k1Sum)).isEqualTo(1);
		Assertions.assertThat(merged.size(k2Sum)).isEqualTo(1);
	}

	// A lateFilter may lead to additional groupBys, which are filtered but suppressed in a later groupBy: while the
	// stream of record is distinct, this constrain is removed when suppressed the lateFiltered column.
	@Test
	public void testDistinct_lateFilter() {
		InMemoryTable tableWrapper = InMemoryTable.builder().distinctSlices(true).build();
		TableQueryV3 tableQuery =
				TableQueryV3.edit(TableQuery.builder().aggregator(k1Sum).aggregator(k2Sum).build()).build();

		// InMemoryTable will produce a single row with both aggregates
		tableWrapper.add(Map.of("k1", 123, "k2", 123));

		ITabularRecordStream stream = tableWrapper.streamSlices(tableQuery);
		TableQueryEngine bootstrapped =
				(TableQueryEngine) engine.bootstrap(QueryPod.forTable(tableWrapper), optimizer, inducer);
		IMultitypeMergeableGrid<ISlice> merged = bootstrapped.mergeTableAggregates(tableQuery, stream);

		Assertions.assertThat(merged.size(k1Sum)).isEqualTo(1);
		Assertions.assertThat(merged.size(k2Sum)).isEqualTo(1);
	}

	@Test
	public void testNotDistinct() {
		InMemoryTable tableWrapper = InMemoryTable.builder().distinctSlices(false).build();
		TableQueryV3 tableQuery =
				TableQueryV3.edit(TableQuery.builder().aggregator(k1Sum).aggregator(k2Sum).build()).build();

		// InMemoryTable will produce two rows, each with one aggregate
		tableWrapper.add(Map.of("k1", 123));
		tableWrapper.add(Map.of("k2", 123));

		ITabularRecordStream stream = tableWrapper.streamSlices(tableQuery);
		TableQueryEngine bootstrapped =
				(TableQueryEngine) engine.bootstrap(QueryPod.forTable(tableWrapper), optimizer, inducer);
		IMultitypeMergeableGrid<ISlice> merged = bootstrapped.mergeTableAggregates(tableQuery, stream);

		Assertions.assertThat(merged.size(k1Sum)).isEqualTo(1);
		Assertions.assertThat(merged.size(k2Sum)).isEqualTo(1);
	}

	// Regression test for `forEachMeasure` with an EmptyAggregation carrying a per-aggregator FILTER: the empty
	// aggregator must materialize a slice ONLY when that slice matches its FILTER. A slice that doesn't match
	// must NOT receive a `valueReceiver.onLong(0)` write — otherwise we'd silently materialize a "slice exists"
	// marker on rows the FILTER explicitly rejected. The merged grid's size for the empty aggregator is the
	// observable signal: with one matching row out of two, size must be 1, not 2.
	@Test
	public void testForEachMeasure_emptyAggregatorFilter_skipsUnmatchedSlice() {
		InMemoryTable tableWrapper = InMemoryTable.builder().distinctSlices(false).build();

		// Two rows on distinct `a` coordinates.
		tableWrapper.add(Map.of("a", "a1", "v", 10));
		tableWrapper.add(Map.of("a", "a2", "v", 20));

		Aggregator empty = Aggregator.empty();
		// Empty aggregator with FILTER `a=a1` — only the a=a1 slice should materialize.
		FilteredAggregator filteredEmpty =
				FilteredAggregator.builder().aggregator(empty).filter(ColumnFilter.matchEq("a", "a1")).build();
		TableQueryV3 tableQuery = TableQueryV3
				.edit(TableQueryV2.builder().aggregator(filteredEmpty).groupBy(GroupByColumns.named("a")).build())
				.build();

		ITabularRecordStream stream = tableWrapper.streamSlices(tableQuery);
		TableQueryEngine bootstrapped =
				(TableQueryEngine) engine.bootstrap(QueryPod.forTable(tableWrapper), optimizer, inducer);
		IMultitypeMergeableGrid<ISlice> merged = bootstrapped.mergeTableAggregates(tableQuery, stream);

		// Only the `a=a1` slice was materialized by the empty aggregator. The `a=a2` slice received no
		// `onLong(0)` write — its FILTER didn't match, so the slice is genuinely absent from this aggregator's
		// column.
		Assertions.assertThat(merged.size(empty)).isEqualTo(1);
	}
}
