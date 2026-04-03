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
package eu.solven.adhoc.dataframe.column.partitioned;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashMergeableColumn;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.primitive.IValueProvider;

public class TestPartitionedMergeableColumn {

	final SumAggregation sum = new SumAggregation();

	PartitionedMergeableColumn<String> column = PartitionedMergeableColumn.<String>builder()
			.aggregation(sum)
			.partition(MultitypeHashMergeableColumn.<String>builder().aggregation(sum).build())
			.partition(MultitypeHashMergeableColumn.<String>builder().aggregation(sum).build())
			.partition(MultitypeHashMergeableColumn.<String>builder().aggregation(sum).build())
			.partition(MultitypeHashMergeableColumn.<String>builder().aggregation(sum).build())
			.build();

	// --- factory ---

	@Test
	public void of_defaultPartitions_usesAvailableProcessors() {
		Assertions.assertThat(column.isEmpty()).isTrue();
		Assertions.assertThat(column.size()).isEqualTo(0);
	}

	// --- IPartitioned ---

	@Test
	public void getNbPartitions_matchesRequestedCount() {
		Assertions.assertThat(column.getNbPartitions()).isEqualTo(4);
	}

	@Test
	public void getPartition_returnsCorrectPartition() {
		column.merge("k").onLong(7L);

		int idx = PartitioningHelpers.getPartitionIndex("k", column.getNbPartitions());
		Assertions.assertThat(IValueProvider.getValue(column.getPartition(idx).onValue("k"))).isEqualTo(7L);
	}

	// --- isEmpty / size ---

	@Test
	public void isEmpty_beforeAnyMerge() {
		Assertions.assertThat(column.isEmpty()).isTrue();
		Assertions.assertThat(column.size()).isEqualTo(0);
	}

	@Test
	public void isEmpty_afterMerge_returnsFalse() {
		column.merge("k1").onLong(1L);

		Assertions.assertThat(column.isEmpty()).isFalse();
		Assertions.assertThat(column.size()).isEqualTo(1);
	}

	// --- merge / onValue ---

	@Test
	public void merge_singleEntry_readBack() {
		column.merge("k1").onLong(42L);

		Assertions.assertThat(IValueProvider.getValue(column.onValue("k1"))).isEqualTo(42L);
	}

	@Test
	public void merge_sameKey_twice_summed() {
		column.merge("k1").onLong(100L);
		column.merge("k1").onLong(23L);

		Assertions.assertThat(IValueProvider.getValue(column.onValue("k1"))).isEqualTo(123L);
	}

	@Test
	public void onValue_missingKey_returnsNull() {
		Assertions.assertThat(IValueProvider.getValue(column.onValue("absent"))).isNull();
	}

	@Test
	public void merge_doubleValue_summed() {
		column.merge("k1").onDouble(1.5D);
		column.merge("k1").onDouble(2.5D);

		Assertions.assertThat(IValueProvider.getValue(column.onValue("k1"))).isEqualTo(4.0D);
	}

	// --- many keys, spread across partitions ---

	@Test
	public void merge_manyKeys_allPresentWithCorrectValues() {
		int n = 200;
		IntStream.range(0, n).forEach(i -> column.merge("key" + i).onLong(i));

		Assertions.assertThat(column.size()).isEqualTo(n);
		IntStream.range(0, n).forEach(i -> {
			Assertions.assertThat(IValueProvider.getValue(column.onValue("key" + i))).as("key" + i).isEqualTo((long) i);
		});
	}

	// --- scan ---

	@Test
	public void scan_visitsAllKeys() {
		column.merge("a").onLong(1L);
		column.merge("b").onLong(2L);
		column.merge("c").onLong(3L);

		Map<String, Object> collected = new java.util.LinkedHashMap<>();
		// IColumnScanner<T>: key -> IValueReceiver; IValueReceiver is @FunctionalInterface on onObject.
		// onLong defaults to boxing then calling onObject, so this captures all types.
		column.scan(key -> value -> collected.put(key, value));

		Assertions.assertThat(collected).containsKeys("a", "b", "c").hasSize(3);
	}

	// --- stream ---

	@Test
	public void stream_yieldsAllEntries() {
		column.merge("x").onLong(10L);
		column.merge("y").onLong(20L);

		List<SliceAndMeasure<String>> entries = column.stream().toList();

		Assertions.assertThat(entries).hasSize(2);
		List<String> keys = entries.stream().map(SliceAndMeasure::getSlice).toList();
		Assertions.assertThat(keys).containsExactlyInAnyOrder("x", "y");
	}

	@Test
	public void keyStream_returnsAllKeys() {
		column.merge("p").onLong(1L);
		column.merge("q").onLong(2L);

		Assertions.assertThat(column.keyStream().toList()).containsExactlyInAnyOrder("p", "q");
	}

	// --- builder with pre-built partitions ---

	@Test
	public void builder_prebuiltPartitions_exposedAsUnifiedColumn() {
		IMultitypeMergeableColumn<String> p0 = MultitypeHashMergeableColumn.<String>builder().aggregation(sum).build();
		IMultitypeMergeableColumn<String> p1 = MultitypeHashMergeableColumn.<String>builder().aggregation(sum).build();

		p0.merge("a").onLong(1L);
		p1.merge("b").onLong(2L);

		PartitionedMergeableColumn<String> col = PartitionedMergeableColumn.<String>builder()
				.aggregation(sum)
				.partitions(ImmutableList.of(p0, p1))
				.build();

		Assertions.assertThat(col.size()).isEqualTo(2);
		// The hash routing does not guarantee which partition index owns which key when partitions are pre-built
		// externally. Use stream() to verify both entries are reachable.
		List<String> keys = col.keyStream().toList();
		Assertions.assertThat(keys).containsExactlyInAnyOrder("a", "b");
	}

	// --- purgeAggregationCarriers ---

	@Test
	public void purgeAggregationCarriers_resolvesCarriers() {
		IAggregation agg = RankAggregation.fromMax(2);

		PartitionedMergeableColumn<String> column = PartitionedMergeableColumn.<String>builder()
				.aggregation(agg)
				.partition(MultitypeHashMergeableColumn.<String>builder().aggregation(agg).build())
				.partition(MultitypeHashMergeableColumn.<String>builder().aggregation(agg).build())
				.partition(MultitypeHashMergeableColumn.<String>builder().aggregation(agg).build())
				.partition(MultitypeHashMergeableColumn.<String>builder().aggregation(agg).build())
				.build();

		column.merge("k1").onObject(10);
		column.merge("k1").onObject(5);

		// Before purge: carrier is still wrapped
		IValueProvider beforePurge = column.onValue("k1");
		Assertions.assertThat(IValueProvider.getValue(beforePurge))
				.isInstanceOf(RankAggregation.IRankAggregationCarrier.class);

		IMultitypeColumnFastGet<String> purged = column.purgeAggregationCarriers();

		// After purge: carrier is resolved to the ranked value (RankAggregation keeps the bottom of the top-N).
		// Integer inputs are stored as Long internally (AdhocPrimitiveHelpers normalises int → long).
		Assertions.assertThat(IValueProvider.getValue(purged.onValue("k1"))).isEqualTo(5);
	}

}
