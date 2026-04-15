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
package eu.solven.adhoc.dataframe.column.navigable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueProviderTestHelpers;
import eu.solven.adhoc.primitive.IValueReceiver;

public class TestMultitypeNavigableIntColumn {

	@Test
	public void testEmpty() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		Assertions.assertThat(column.size()).isZero();
		Assertions.assertThat(column.isEmpty()).isTrue();
		Assertions.assertThat(column.sortedPrefixLength()).isZero();
		Assertions.assertThat(IValueProvider.getValue(column.onValue(42))).isNull();
	}

	@Test
	public void testAppendAscending_long() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(2).onLong(20L);
		column.append(5).onLong(50L);

		Assertions.assertThat(column.size()).isEqualTo(3);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(column.onValue(1))).isEqualTo(10L);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(column.onValue(2))).isEqualTo(20L);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(column.onValue(5))).isEqualTo(50L);
	}

	@Test
	public void testAppendAscending_mixedTypes() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(123L);
		column.append(2).onDouble(23.45D);
		column.append(3).onObject("hello");

		// MultitypeArray promotes to Object storage as soon as it sees heterogeneous writes, so all reads go through
		// onObject. Use the generic IValueProvider.getValue accessor to stay type-agnostic.
		Assertions.assertThat(column.size()).isEqualTo(3);
		Assertions.assertThat(IValueProvider.getValue(column.onValue(1))).isEqualTo(123L);
		Assertions.assertThat(IValueProvider.getValue(column.onValue(2))).isEqualTo(23.45D);
		Assertions.assertThat(IValueProvider.getValue(column.onValue(3))).isEqualTo("hello");
	}

	@Test
	public void testOnValue_unknownKey_returnsNull() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(10).onLong(100L);
		column.append(20).onLong(200L);

		Assertions.assertThat(IValueProvider.getValue(column.onValue(5))).isNull();
		Assertions.assertThat(IValueProvider.getValue(column.onValue(15))).isNull();
		Assertions.assertThat(IValueProvider.getValue(column.onValue(25))).isNull();
	}

	@Test
	public void testRandomInsertion_descendingKeys_sortsInAscendingOrder() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(3).onLong(30L);
		column.append(1).onLong(10L);
		column.append(2).onLong(20L);

		Assertions.assertThat(column.size()).isEqualTo(3);

		List<Integer> keys = new ArrayList<>();
		List<Long> values = new ArrayList<>();
		column.scan(key -> new IValueReceiver() {
			@Override
			public void onLong(long v) {
				keys.add(key);
				values.add(v);
			}

			@Override
			public void onObject(Object v) {
				throw new UnsupportedOperationException("unexpected onObject=" + v);
			}
		});

		Assertions.assertThat(keys).containsExactly(1, 2, 3);
		Assertions.assertThat(values).containsExactly(10L, 20L, 30L);
	}

	@Test
	public void testAppendIfOptimal_ascending_isOptimal() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);

		Optional<IValueReceiver> optimal = column.appendIfOptimal(2);
		Assertions.assertThat(optimal).isPresent();
		optimal.get().onLong(20L);

		Assertions.assertThat(column.size()).isEqualTo(2);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(column.onValue(2))).isEqualTo(20L);
	}

	@Test
	public void testAppendIfOptimal_outOfOrder_rejected() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(5).onLong(50L);
		column.append(10).onLong(100L);

		Optional<IValueReceiver> rejected = column.appendIfOptimal(3);
		Assertions.assertThat(rejected).isEmpty();

		Assertions.assertThat(column.size()).isEqualTo(2);
		Assertions.assertThat(IValueProvider.getValue(column.onValue(3))).isNull();
	}

	@Test
	public void testAppendIfOptimal_existingKey_throws() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(3).onLong(30L);

		// appendIfOptimal on the last key routes through the merge path, which is unsupported on an append-only
		// column.
		Assertions.assertThatThrownBy(() -> column.appendIfOptimal(3))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("does not allow merging");
		// Same for a key present but not at the end: exact lookup finds the key, then hits merge and throws.
		Assertions.assertThatThrownBy(() -> column.appendIfOptimal(1))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("does not allow merging");
	}

	@Test
	public void testStream_returnsSliceAndMeasureInAscendingOrder() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(3).onLong(30L);
		column.append(2).onLong(20L);

		List<Integer> slices = column.stream().toList().stream().map(sm -> sm.getSlice()).toList();
		Assertions.assertThat(slices).containsExactly(1, 2, 3);

		List<Long> values = column.stream()
				.toList()
				.stream()
				.map(sm -> IValueProviderTestHelpers.getLong(sm.getValueProvider()))
				.toList();
		Assertions.assertThat(values).containsExactly(10L, 20L, 30L);
	}

	@Test
	public void testStream_strategy_sorted_sub_returnsAll() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(2).onLong(20L);

		Assertions.assertThat(column.stream(StreamStrategy.SORTED_SUB).count()).isEqualTo(2);
		Assertions.assertThat(column.size(StreamStrategy.SORTED_SUB)).isEqualTo(2);
	}

	@Test
	public void testStream_strategy_complement_isEmpty() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(2).onLong(20L);

		Assertions.assertThat(column.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).count()).isZero();
		Assertions.assertThat(column.size(StreamStrategy.SORTED_SUB_COMPLEMENT)).isZero();
	}

	@Test
	public void testLimitSkip() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(2).onLong(20L);
		column.append(3).onLong(30L);

		List<Integer> limited = column.limit(2).toList().stream().map(sm -> sm.getSlice()).toList();
		Assertions.assertThat(limited).containsExactly(1, 2);

		List<Integer> skipped = column.skip(1).toList().stream().map(sm -> sm.getSlice()).toList();
		Assertions.assertThat(skipped).containsExactly(2, 3);
	}

	@Test
	public void testKeyStream_returnsAllKeys() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(2).onLong(20L);
		column.append(3).onLong(30L);

		List<Integer> keys = column.keyStream().toList();
		Assertions.assertThat(keys).containsExactly(1, 2, 3);
	}

	@Test
	public void testMergeSameKey_throws() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);

		// Re-appending the same (already-last) key attempts a merge, which is unsupported.
		Assertions.assertThatThrownBy(() -> column.append(1).onLong(20L)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testLocked_cannotAppend() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		// scan() triggers doLock() internally.
		column.scan(key -> new IValueReceiver() {
			@Override
			public void onLong(long v) {
				// no-op
			}

			@Override
			public void onObject(Object v) {
				// no-op
			}
		});

		Assertions.assertThatThrownBy(() -> column.append(2).onLong(20L)).isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void testPurgeAggregationCarriers_returnsCopy() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(2).onLong(20L);

		Assertions.assertThat(column.purgeAggregationCarriers()).isNotSameAs(column);
		Assertions.assertThat(column.purgeAggregationCarriers().size()).isEqualTo(2);
	}

	@Test
	public void testEmpty_factory() {
		MultitypeNavigableIntColumn empty = MultitypeNavigableIntColumn.empty();

		Assertions.assertThat(empty.size()).isZero();
		Assertions.assertThat(empty.isEmpty()).isTrue();
		Assertions.assertThatThrownBy(() -> empty.append(1).onLong(10L)).isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void testCompact_doesNotThrow() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().capacity(128).build();

		column.append(1).onLong(10L);
		column.append(2).onLong(20L);

		column.compact();

		Assertions.assertThat(column.size()).isEqualTo(2);
	}

	@Test
	public void testNullAppend_isPurged() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(2).onObject(null);

		// The null append is lazily cleared on the next size()/append call.
		Assertions.assertThat(column.size()).isEqualTo(1);
		Assertions.assertThat(IValueProvider.getValue(column.onValue(2))).isNull();
	}

	@Test
	public void testToString_includesSize() {
		MultitypeNavigableIntColumn column = MultitypeNavigableIntColumn.builder().build();

		column.append(1).onLong(10L);
		column.append(2).onLong(20L);

		Assertions.assertThat(column.toString()).contains("size=2");
	}
}
