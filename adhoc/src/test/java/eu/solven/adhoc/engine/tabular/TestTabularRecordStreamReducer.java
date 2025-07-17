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
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.InMemoryTable;

public class TestTabularRecordStreamReducer implements IAdhocTestConstants {
	TableQueryEngine engine = TableQueryEngine.builder().build();

	ITableQueryOptimizer tableQueryOptimizer =
			engine.optimizerFactory.makeOptimizer(engine.getFactories(), () -> Set.of());

	TableQueryEngineBootstrapped bootstrapped = engine.bootstrap(tableQueryOptimizer);

	@Test
	public void testDistinct() {
		InMemoryTable tableWrapper = InMemoryTable.builder().distinctSlices(true).build();
		Set<TableQuery> tableQueryV1 = Set.of(TableQuery.builder().aggregator(k1Sum).aggregator(k2Sum).build());
		TableQueryV2 tableQuery = Iterables.getOnlyElement(TableQueryV2.fromV1(tableQueryV1));

		// InMemoryTable will produce a single row with both aggregates
		tableWrapper.add(Map.of("k1", 123, "k2", 123));

		ITabularRecordStream stream = tableWrapper.streamSlices(tableQuery);
		IMultitypeMergeableGrid<IAdhocSlice> merged =
				bootstrapped.mergeTableAggregates(QueryPod.forTable(tableWrapper), tableQuery, stream);

		Assertions.assertThat(merged.size(k1Sum)).isEqualTo(1);
		Assertions.assertThat(merged.size(k2Sum)).isEqualTo(1);
	}

	// A lateFilter may lead to additional groupBys, which are filtered but suppressed in a later groupBy: while the
	// stream of record is distinct, this constrain is removed when suppressed the lateFiltered column.
	@Test
	public void testDistinct_lateFilter() {
		InMemoryTable tableWrapper = InMemoryTable.builder().distinctSlices(true).build();
		Set<TableQuery> tableQueryV1 = Set.of(TableQuery.builder().aggregator(k1Sum).aggregator(k2Sum).build());
		TableQueryV2 tableQuery = Iterables.getOnlyElement(TableQueryV2.fromV1(tableQueryV1));

		// InMemoryTable will produce a single row with both aggregates
		tableWrapper.add(Map.of("k1", 123, "k2", 123));

		ITabularRecordStream stream = tableWrapper.streamSlices(tableQuery);
		IMultitypeMergeableGrid<IAdhocSlice> merged =
				bootstrapped.mergeTableAggregates(QueryPod.forTable(tableWrapper), tableQuery, stream);

		Assertions.assertThat(merged.size(k1Sum)).isEqualTo(1);
		Assertions.assertThat(merged.size(k2Sum)).isEqualTo(1);
	}

	@Test
	public void testNotDistinct() {
		InMemoryTable tableWrapper = InMemoryTable.builder().distinctSlices(false).build();
		Set<TableQuery> tableQueryV1 = Set.of(TableQuery.builder().aggregator(k1Sum).aggregator(k2Sum).build());
		TableQueryV2 tableQuery = Iterables.getOnlyElement(TableQueryV2.fromV1(tableQueryV1));

		// InMemoryTable will produce two rows, each with one aggregate
		tableWrapper.add(Map.of("k1", 123));
		tableWrapper.add(Map.of("k2", 123));

		ITabularRecordStream stream = tableWrapper.streamSlices(tableQuery);
		IMultitypeMergeableGrid<IAdhocSlice> merged =
				bootstrapped.mergeTableAggregates(QueryPod.forTable(tableWrapper), tableQuery, stream);

		Assertions.assertThat(merged.size(k1Sum)).isEqualTo(1);
		Assertions.assertThat(merged.size(k2Sum)).isEqualTo(1);
	}
}
