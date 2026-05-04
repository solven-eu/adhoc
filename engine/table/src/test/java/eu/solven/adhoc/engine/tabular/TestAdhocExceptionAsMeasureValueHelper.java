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
package eu.solven.adhoc.engine.tabular;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSetMultimap;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.model.query.IGroupBy;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestAdhocExceptionAsMeasureValueHelper {

	final FilteredAggregator aggSum = FilteredAggregator.builder().aggregator(Aggregator.sum("price")).build();
	final FilteredAggregator aggCount = FilteredAggregator.builder().aggregator(Aggregator.countAsterisk()).build();

	final IGroupBy gbCountry = GroupByColumns.named("country");
	final IGroupBy gbCity = GroupByColumns.named("city");

	@Test
	public void testAsMap_emptyColumns_returnsEmptyMap() {
		Map<String, ?> map = AdhocExceptionAsMeasureValueHelper.asMap(new TreeSet<>());

		Assertions.assertThat(map).isEmpty();
	}

	@Test
	public void testAsMap_singleColumn_keyMappedToErrorMarker() {
		// The "error" marker is what the EXCEPTIONS_AS_MEASURE_VALUE option emits as the slice coordinate
		// for every grouped-by column on the synthetic error row.
		NavigableSet<String> cols = new TreeSet<>(List.of("country"));

		Map<String, ?> map = AdhocExceptionAsMeasureValueHelper.asMap(cols);

		Assertions.assertThat((Map) map).containsEntry("country", "error").hasSize(1);
	}

	@Test
	public void testAsMap_multipleColumns_eachMappedToErrorMarker() {
		NavigableSet<String> cols = new TreeSet<>(List.of("country", "city"));

		Map<String, ?> map = AdhocExceptionAsMeasureValueHelper.asMap(cols);

		Assertions.assertThat((Map) map).containsEntry("country", "error").containsEntry("city", "error").hasSize(2);
	}

	@Test
	public void testAsSlice_emptyColumns() {
		ISlice slice = AdhocExceptionAsMeasureValueHelper.asSlice(new TreeSet<>());

		Assertions.assertThat(slice.getCoordinates()).isEmpty();
	}

	@Test
	public void testAsSlice_columnsWrappedAsSlice() {
		NavigableSet<String> cols = new TreeSet<>(List.of("country"));

		ISlice slice = AdhocExceptionAsMeasureValueHelper.asSlice(cols);

		Assertions.assertThat((Map) slice.getCoordinates()).containsEntry("country", "error").hasSize(1);
	}

	@Test
	public void testMakeErrorStream_singleGroupBy_singleAggregator() {
		// One groupBy → one error record. The aggregate value carries the original Map.Entry from the
		// groupByToAggregators multimap (this is how downstream callers later identify which groupBy raised
		// the error — it's stored by the helper as-is rather than as the Throwable directly).
		TableQueryV4 query =
				TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum)).build();
		Throwable error = new IllegalStateException("boom");

		ITabularRecordStream stream = AdhocExceptionAsMeasureValueHelper.makeErrorStream(query, error);

		Assertions.assertThat(stream.isDistinctSlices()).isTrue();
		List<Map<String, ?>> records = stream.toList();
		Assertions.assertThat(records).hasSize(1);
		Assertions.assertThat((Map) records.getFirst())
				.containsEntry("country", "error")
				.containsKey(aggSum.getAlias());
	}

	@Test
	public void testMakeErrorStream_multipleGroupBys_oneRecordPerGroupBy() {
		TableQueryV4 query = TableQueryV4.builder()
				.groupByToAggregator(gbCountry, aggSum)
				.groupByToAggregator(gbCity, aggCount)
				.build();
		Throwable error = new IllegalStateException("boom");

		ITabularRecordStream stream = AdhocExceptionAsMeasureValueHelper.makeErrorStream(query, error);

		List<Map<String, ?>> records = stream.toList();
		Assertions.assertThat(records).hasSize(2);
		Assertions.assertThat(records)
				.anySatisfy(m -> Assertions.assertThat((Map) m).containsEntry("country", "error"))
				.anySatisfy(m -> Assertions.assertThat((Map) m).containsEntry("city", "error"));
	}

	@Test
	public void testMakeErrorStream_recordCarriesAggregateAlias() {
		// The helper writes a value (the Map.Entry of groupBy→aggregators) under the aggregator's alias key,
		// so downstream callers can read it via `record.getAggregate(alias)`.
		TableQueryV4 query =
				TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum)).build();
		Throwable error = new IllegalStateException("boom");

		ITabularRecordStream stream = AdhocExceptionAsMeasureValueHelper.makeErrorStream(query, error);

		List<ITabularRecord> records = stream.records().toList();
		Assertions.assertThat(records).hasSize(1);
		ITabularRecord record = records.getFirst();
		Assertions.assertThat(record.aggregateKeySet()).containsExactly(aggSum.getAlias());
		Assertions.assertThat(record.getAggregate(aggSum.getAlias())).isNotNull();
	}

	@Test
	public void testMakeErrorStream_close_isNoOp() {
		// `close()` is documented as a no-op (no resources held) — must not throw even on repeated invocation.
		TableQueryV4 query =
				TableQueryV4.builder().groupByToAggregators(ImmutableSetMultimap.of(gbCountry, aggSum)).build();
		ITabularRecordStream stream = AdhocExceptionAsMeasureValueHelper.makeErrorStream(query, new RuntimeException());

		Assertions.assertThatCode(() -> {
			stream.close();
			stream.close();
		}).doesNotThrowAnyException();
	}
}
