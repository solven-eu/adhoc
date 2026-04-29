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
package eu.solven.adhoc.table;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV3;

/**
 * Unit tests for {@link InMemoryTable#streamRows(ITableQueryPod, TableQueryV3)} — the per-row contract used by the
 * DRILLTHROUGH path: each stored row matching the WHERE produces one {@link ITabularRecord}, no slice collapse, and the
 * per-aggregator FILTER gates the alias on a per-row basis.
 */
public class TestInMemoryTable_streamRawRows implements IAdhocTestConstants {

	private InMemoryTable newTable() {
		return InMemoryTable.builder().distinctSlices(false).build();
	}

	private List<ITabularRecord> drainRecords(InMemoryTable table, TableQueryV3 query) {
		try (ITabularRecordStream stream = table.streamRows(ITableQueryPod.forTable(table), query)) {
			return stream.records().toList();
		}
	}

	/**
	 * Three rows on overlapping coordinates ({@code a=a1, a=a1, a=a2}). {@code streamRawRows} must surface them as
	 * three distinct records — no collapse on the {@code a=a1} slice.
	 */
	@Test
	public void testStreamRawRows_threeRows_noCollapse() {
		InMemoryTable table = newTable();
		table.add(Map.of("a", "a1", "k1", 10));
		table.add(Map.of("a", "a1", "k1", 20));
		table.add(Map.of("a", "a2", "k1", 30));

		FilteredAggregator k1Filtered = FilteredAggregator.builder().aggregator(k1Sum).build();
		TableQueryV3 query = TableQueryV3.builder().groupBy(GroupByColumns.named("a")).aggregator(k1Filtered).build();

		List<ITabularRecord> records = drainRecords(table, query);
		Assertions.assertThat(records).hasSize(3);
		Assertions.assertThat(records.stream().map(r -> (Object) r.aggregatesAsMap().get(k1Sum.getAlias())).toList())
				.containsExactlyInAnyOrder(10, 20, 30);
	}

	/**
	 * The WHERE filter is honoured: only rows matching {@code a=a1} survive. No GROUP BY collapse happens — both
	 * surviving rows are surfaced.
	 */
	@Test
	public void testStreamRawRows_whereFilterApplied() {
		InMemoryTable table = newTable();
		table.add(Map.of("a", "a1", "k1", 10));
		table.add(Map.of("a", "a1", "k1", 20));
		table.add(Map.of("a", "a2", "k1", 30));

		FilteredAggregator k1Filtered = FilteredAggregator.builder().aggregator(k1Sum).build();
		TableQueryV3 query = TableQueryV3.builder()
				.filter(ColumnFilter.matchEq("a", "a1"))
				.groupBy(GroupByColumns.named("a"))
				.aggregator(k1Filtered)
				.build();

		List<ITabularRecord> records = drainRecords(table, query);
		Assertions.assertThat(records).hasSize(2);
		Assertions.assertThat(records.stream().map(r -> (Object) r.aggregatesAsMap().get(k1Sum.getAlias())).toList())
				.containsExactlyInAnyOrder(10, 20);
	}

	/**
	 * Per-aggregator FILTER is applied per-row: rows for which the FILTER does not match must NOT have the alias
	 * populated, but the row itself stays in the stream (other aggregators may apply).
	 */
	@Test
	public void testStreamRawRows_perAggregatorFilter_perRow() {
		InMemoryTable table = newTable();
		table.add(Map.of("a", "a1", "k1", 10));
		table.add(Map.of("a", "a2", "k1", 20));
		table.add(Map.of("a", "a3", "k1", 30));

		FilteredAggregator k1ForA1 =
				FilteredAggregator.builder().aggregator(k1Sum).filter(ColumnFilter.matchEq("a", "a1")).build();
		IGroupBy groupBy = GroupByColumns.named("a");
		TableQueryV3 query = TableQueryV3.builder().groupBy(groupBy).aggregator(k1ForA1).build();

		List<ITabularRecord> records = drainRecords(table, query);
		Assertions.assertThat(records).hasSize(3);

		// Only the a=a1 row carries the k1 alias; the others have it absent.
		long withAlias = records.stream().filter(r -> r.aggregateKeySet().contains(k1Sum.getAlias())).count();
		Assertions.assertThat(withAlias).isEqualTo(1);
	}
}
