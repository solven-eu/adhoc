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
package eu.solven.adhoc.dataframe.column;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableIntColumn;
import eu.solven.adhoc.primitive.IValueProvider;

public class TestUndictionarizedColumn {

	/**
	 * Builds an UndictionarizedColumn over a small dictionarized column with the slices in <em>insertion</em> order:
	 * indices [0..size) ↔ slices in caller order, regardless of slice ordering.
	 */
	private UndictionarizedColumn<String> buildColumn(List<String> slicesInInsertionOrder,
			List<Long> valuesInInsertionOrder,
			int sortedLength) {
		Assertions.assertThat(slicesInInsertionOrder).hasSameSizeAs(valuesInInsertionOrder);

		MultitypeNavigableIntColumn dict = MultitypeNavigableIntColumn.builder().build();
		for (int i = 0; i < slicesInInsertionOrder.size(); i++) {
			dict.append(i).onLong(valuesInInsertionOrder.get(i));
		}

		return UndictionarizedColumn.<String>builder()
				.column(dict)
				.indexToSlice(slicesInInsertionOrder::get)
				.sliceToIndex(slicesInInsertionOrder::indexOf)
				.sortedLength(sortedLength)
				.sortedLeg(i -> i < sortedLength)
				.build();
	}

	// ---- legacy behavior: sortedPrefixLength == 0 ----

	@Test
	public void testLegacy_sortedSubIsEmpty_complementHasEverything() {
		UndictionarizedColumn<String> col = buildColumn(List.of("a", "b", "c"), List.of(1L, 2L, 3L), 0);

		Assertions.assertThat(col.stream(StreamStrategy.SORTED_SUB).toList()).isEmpty();
		Assertions.assertThat(col.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList())
				.extracting(SliceAndMeasure::getSlice)
				.containsExactly("a", "b", "c");

		Assertions.assertThat(IValueProvider.getValue(col.onValue("a", StreamStrategy.SORTED_SUB))).isNull();
		Assertions.assertThat(IValueProvider.getValue(col.onValue("a", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isEqualTo(1L);
	}

	// ---- fully-sorted: sortedPrefixLength == size ----

	@Test
	public void testFullySorted_sortedSubHasEverything_complementIsEmpty() {
		UndictionarizedColumn<String> col = buildColumn(List.of("a", "b", "c"), List.of(1L, 2L, 3L), 3);

		Assertions.assertThat(col.stream(StreamStrategy.SORTED_SUB).toList())
				.extracting(SliceAndMeasure::getSlice)
				.containsExactly("a", "b", "c");
		Assertions.assertThat(col.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList()).isEmpty();

		Assertions.assertThat(IValueProvider.getValue(col.onValue("b", StreamStrategy.SORTED_SUB))).isEqualTo(2L);
		Assertions.assertThat(IValueProvider.getValue(col.onValue("b", StreamStrategy.SORTED_SUB_COMPLEMENT))).isNull();
	}

	// ---- partial: 3 sorted + 2 unordered tail ----

	@Test
	public void testPartial_splitsCorrectly() {
		// First 3 slices ("a", "b", "c") form a slice-sorted prefix; the next 2 ("a2", "z") are unordered tail
		// (relative to the prefix's "c").
		UndictionarizedColumn<String> col =
				buildColumn(List.of("a", "b", "c", "a2", "z"), List.of(1L, 2L, 3L, 4L, 5L), 3);

		List<SliceAndMeasure<String>> sorted = col.stream(StreamStrategy.SORTED_SUB).toList();
		List<SliceAndMeasure<String>> complement = col.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList();

		Assertions.assertThat(sorted).extracting(SliceAndMeasure::getSlice).containsExactly("a", "b", "c");
		Assertions.assertThat(complement).extracting(SliceAndMeasure::getSlice).containsExactly("a2", "z");

		// Prefix slice on COMPLEMENT → NULL.
		Assertions.assertThat(IValueProvider.getValue(col.onValue("a", StreamStrategy.SORTED_SUB_COMPLEMENT))).isNull();
		// Tail slice on SORTED_SUB → NULL.
		Assertions.assertThat(IValueProvider.getValue(col.onValue("a2", StreamStrategy.SORTED_SUB))).isNull();
		// Each on its own side → real value.
		Assertions.assertThat(IValueProvider.getValue(col.onValue("a", StreamStrategy.SORTED_SUB))).isEqualTo(1L);
		Assertions.assertThat(IValueProvider.getValue(col.onValue("a2", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isEqualTo(4L);
	}

	// ---- ALL strategy unaffected by sortedPrefixLength ----

	@Test
	public void testStreamAllReturnsEverything_regardlessOfPrefix() {
		UndictionarizedColumn<String> col = buildColumn(List.of("c", "a", "b"), List.of(3L, 1L, 2L), 1);

		List<SliceAndMeasure<String>> all = col.stream(StreamStrategy.ALL).toList();
		Assertions.assertThat(all).extracting(SliceAndMeasure::getSlice).containsExactlyInAnyOrder("a", "b", "c");
	}

	// ---- purgeAggregationCarriers propagates the prefix ----

	@Test
	public void testPurgeAggregationCarriers_propagatesSortedPrefixLength() {
		UndictionarizedColumn<String> col = buildColumn(List.of("a", "b", "c"), List.of(1L, 2L, 3L), 2);

		IMultitypeColumnFastGet<String> purged = col.purgeAggregationCarriers();
		Assertions.assertThat(purged).isInstanceOf(UndictionarizedColumn.class);

		// The purged copy must still expose the same sorted leg.
		Assertions.assertThat(purged.stream(StreamStrategy.SORTED_SUB).toList())
				.extracting(SliceAndMeasure::getSlice)
				.containsExactly("a", "b");
	}

	// ---- int-specialized inner column: values are read without Integer boxing ----

	@Test
	public void testWrapsIntSpecializedColumn_onValueReadsCorrectly() {
		MultitypeNavigableIntColumn intDict = MultitypeNavigableIntColumn.builder().build();
		intDict.append(0).onLong(100L);
		intDict.append(1).onLong(200L);
		intDict.append(2).onLong(300L);

		List<String> slices = List.of("alpha", "beta", "gamma");
		UndictionarizedColumn<String> col = UndictionarizedColumn.<String>builder()
				.column(intDict)
				.indexToSlice(slices::get)
				.sliceToIndex(slices::indexOf)
				.sortedLength(3)
				.sortedLeg(i -> i < 3)
				.build();

		Assertions.assertThat(IValueProvider.getValue(col.onValue("alpha"))).isEqualTo(100L);
		Assertions.assertThat(IValueProvider.getValue(col.onValue("beta"))).isEqualTo(200L);
		Assertions.assertThat(IValueProvider.getValue(col.onValue("gamma"))).isEqualTo(300L);

		// Strategy overrides also go through the primitive path.
		Assertions.assertThat(IValueProvider.getValue(col.onValue("alpha", StreamStrategy.SORTED_SUB))).isEqualTo(100L);
		Assertions.assertThat(IValueProvider.getValue(col.onValue("alpha", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isNull();
		Assertions.assertThat(IValueProvider.getValue(col.onValue("gamma", StreamStrategy.ALL))).isEqualTo(300L);
	}

	@Test
	public void testWrapsIntSpecializedColumn_purgeAggregationCarriersRetainsSpecialization() {
		MultitypeNavigableIntColumn intDict = MultitypeNavigableIntColumn.builder().build();
		intDict.append(0).onLong(1L);
		intDict.append(1).onLong(2L);

		List<String> slices = List.of("a", "b");
		UndictionarizedColumn<String> col = UndictionarizedColumn.<String>builder()
				.column(intDict)
				.indexToSlice(slices::get)
				.sliceToIndex(slices::indexOf)
				.sortedLength(2)
				.sortedLeg(i -> i < 2)
				.build();

		// Round-trip through purgeAggregationCarriers must keep producing correct values — the re-wrapped inner
		// column is still the int-specialized variant, so the builder's custom column() setter re-detects it.
		IMultitypeColumnFastGet<String> purged = col.purgeAggregationCarriers();

		Assertions.assertThat(IValueProvider.getValue(purged.onValue("a"))).isEqualTo(1L);
		Assertions.assertThat(IValueProvider.getValue(purged.onValue("b"))).isEqualTo(2L);
	}

}
