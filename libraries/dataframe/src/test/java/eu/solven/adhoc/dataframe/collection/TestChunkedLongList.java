/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.dataframe.collection;

import java.lang.reflect.Field;
import java.util.stream.LongStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.collection.FrozenException;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class TestChunkedLongList {

	static final int BASE = ChunkedArrays.BASE_DEFAULT;

	// --- constructors ---

	@Test
	public void testDefaultConstructor_empty() {
		ChunkedLongList list = new ChunkedLongList();

		Assertions.assertThat(list.size()).isZero();
		Assertions.assertThat(list.isEmpty()).isTrue();
	}

	@Test
	public void testCapacityConstructor_zero() {
		ChunkedLongList list = new ChunkedLongList(0);

		Assertions.assertThat(list.isEmpty()).isTrue();
		list.add(42L);
		Assertions.assertThat(list.getLong(0)).isEqualTo(42L);
	}

	@Test
	public void testCapacityConstructor_negative_throws() {
		Assertions.assertThatThrownBy(() -> new ChunkedLongList(-1))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("-1");
	}

	@Test
	public void testCapacityConstructor_aboveBase() {
		ChunkedLongList list = new ChunkedLongList(300);
		LongStream.range(0, 300).forEach(list::add);

		Assertions.assertThat(list.size()).isEqualTo(300);
		Assertions.assertThat(list.getLong(299)).isEqualTo(299L);
	}

	// --- lazy head initialisation ---

	@Test
	public void testLazyInit_headIsNull() throws Exception {
		ChunkedLongList list = new ChunkedLongList();

		Field f = ChunkedLongList.class.getDeclaredField("head");
		f.setAccessible(true);
		Assertions.assertThat(f.get(list)).isNull();
	}

	@Test
	public void testLazyInit_headAllocatedOnFirstAdd() throws Exception {
		ChunkedLongList list = new ChunkedLongList();
		Field f = ChunkedLongList.class.getDeclaredField("head");
		f.setAccessible(true);

		Assertions.assertThat(f.get(list)).isNull();
		list.add(1L);
		Assertions.assertThat(f.get(list)).isNotNull();
	}

	// --- add / getLong ---

	@Test
	public void testAdd_andGet_sequential() {
		ChunkedLongList list = new ChunkedLongList();
		for (long i = 0; i < 100; i++) {
			list.add(i);
		}

		Assertions.assertThat(list.size()).isEqualTo(100);
		for (int i = 0; i < 100; i++) {
			Assertions.assertThat(list.getLong(i)).isEqualTo(i);
		}
	}

	@Test
	public void testAdd_zero_andNegative() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(0L);
		list.add(-1L);
		list.add(Long.MIN_VALUE);
		list.add(Long.MAX_VALUE);

		Assertions.assertThat(list.getLong(0)).isEqualTo(0L);
		Assertions.assertThat(list.getLong(1)).isEqualTo(-1L);
		Assertions.assertThat(list.getLong(2)).isEqualTo(Long.MIN_VALUE);
		Assertions.assertThat(list.getLong(3)).isEqualTo(Long.MAX_VALUE);
	}

	@Test
	public void testGet_outOfBounds_throws() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(1L);

		Assertions.assertThatThrownBy(() -> list.getLong(1)).isInstanceOf(IndexOutOfBoundsException.class);
		Assertions.assertThatThrownBy(() -> list.getLong(-1)).isInstanceOf(IndexOutOfBoundsException.class);
	}

	// --- head/tail boundary ---

	@Test
	public void testHeadBoundary_lastHeadAndFirstTail() {
		ChunkedLongList list = new ChunkedLongList();
		LongStream.range(0, BASE + 1).forEach(list::add);

		Assertions.assertThat(list.getLong(BASE - 1)).isEqualTo(BASE - 1);
		Assertions.assertThat(list.getLong(BASE)).isEqualTo(BASE);
	}

	@Test
	public void testTailChunkBoundaries() {
		ChunkedLongList list = new ChunkedLongList();
		LongStream.range(0, 1024).forEach(list::add);

		int[] boundaries = { 0, 127, 128, 255, 256, 511, 512, 1023 };
		for (int i : boundaries) {
			Assertions.assertThat(list.getLong(i)).as("index %d", i).isEqualTo(i);
		}
	}

	// --- set ---

	@Test
	public void testSet_returnsOldValue() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(10L);
		list.add(20L);
		list.add(30L);

		long old = list.set(1, 99L);

		Assertions.assertThat(old).isEqualTo(20L);
		Assertions.assertThat(list.getLong(1)).isEqualTo(99L);
		Assertions.assertThat(list.size()).isEqualTo(3);
	}

	@Test
	public void testSet_inTail() {
		ChunkedLongList list = new ChunkedLongList();
		LongStream.range(0, 300).forEach(list::add);

		list.set(200, 999L);

		Assertions.assertThat(list.getLong(200)).isEqualTo(999L);
		Assertions.assertThat(list.getLong(199)).isEqualTo(199L);
		Assertions.assertThat(list.getLong(201)).isEqualTo(201L);
	}

	// --- add at index / remove ---

	@Test
	public void testAddAtIndex_shiftsRight() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(0L);
		list.add(1L);
		list.add(3L);

		list.add(2, 2L);

		Assertions.assertThat(list.toLongArray()).containsExactly(0L, 1L, 2L, 3L);
	}

	@Test
	public void testRemove_shiftsLeft() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(10L);
		list.add(20L);
		list.add(30L);

		long removed = list.removeLong(1);

		Assertions.assertThat(removed).isEqualTo(20L);
		Assertions.assertThat(list.toLongArray()).containsExactly(10L, 30L);
	}

	@Test
	public void testRemove_outOfBounds_throws() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(1L);

		Assertions.assertThatThrownBy(() -> list.removeLong(1)).isInstanceOf(IndexOutOfBoundsException.class);
	}

	// --- clear ---

	@Test
	public void testClear_headOnly() {
		ChunkedLongList list = new ChunkedLongList();
		LongStream.range(0, 50).forEach(list::add);
		list.clear();

		Assertions.assertThat(list.isEmpty()).isTrue();
		list.add(7L);
		Assertions.assertThat(list.getLong(0)).isEqualTo(7L);
	}

	@Test
	public void testClear_withTail() {
		ChunkedLongList list = new ChunkedLongList();
		LongStream.range(0, 500).forEach(list::add);
		list.clear();

		Assertions.assertThat(list.isEmpty()).isTrue();
	}

	// --- compact ---

	@Test
	public void testIsFrozen_falseBeforeCompact() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(1L);

		Assertions.assertThat(list.isFrozen()).isFalse();
	}

	@Test
	public void testIsFrozen_trueAfterCompact() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(1L);
		list.compact();

		Assertions.assertThat(list.isFrozen()).isTrue();
	}

	@Test
	public void testCompact_emptyList_noNpe() {
		ChunkedLongList list = new ChunkedLongList();
		list.compact();

		Assertions.assertThat(list.isEmpty()).isTrue();
		Assertions.assertThat(list.isFrozen()).isTrue();
	}

	@Test
	public void testCompact_trimsHead_twoElements() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(10L);
		list.add(20L);
		list.compact();

		Assertions.assertThat(list.size()).isEqualTo(2);
		Assertions.assertThat(list.getLong(0)).isEqualTo(10L);
		Assertions.assertThat(list.getLong(1)).isEqualTo(20L);
	}

	@Test
	public void testCompact_headOnly_readsCorrectly() {
		ChunkedLongList list = new ChunkedLongList();
		LongStream.range(0, 50).forEach(list::add);
		list.compact();

		Assertions.assertThat(list.size()).isEqualTo(50);
		for (int i = 0; i < 50; i++) {
			Assertions.assertThat(list.getLong(i)).isEqualTo(i);
		}
	}

	@Test
	public void testCompact_withTail_readsCorrectly() {
		ChunkedLongList list = new ChunkedLongList();
		LongStream.range(0, 300).forEach(list::add);
		list.compact();

		Assertions.assertThat(list.size()).isEqualTo(300);
		for (int i = 0; i < 300; i++) {
			Assertions.assertThat(list.getLong(i)).isEqualTo(i);
		}
	}

	@Test
	public void testCompact_preventsAdd() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(1L);
		list.compact();

		Assertions.assertThatThrownBy(() -> list.add(2L)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsAddAtIndex() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(1L);
		list.compact();

		Assertions.assertThatThrownBy(() -> list.add(0, 2L)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsSet() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(1L);
		list.compact();

		Assertions.assertThatThrownBy(() -> list.set(0, 99L)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsRemove() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(1L);
		list.compact();

		Assertions.assertThatThrownBy(() -> list.removeLong(0)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsClear() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(1L);
		list.compact();

		Assertions.assertThatThrownBy(list::clear).isInstanceOf(FrozenException.class);
	}

	// --- iterator / CME ---

	@Test
	public void testIterator_traversesAllElements() {
		ChunkedLongList list = new ChunkedLongList();
		list.add(10L);
		list.add(20L);
		list.add(30L);

		long sum = 0;
		LongIterator it = list.iterator();
		while (it.hasNext()) {
			sum += it.nextLong();
		}

		Assertions.assertThat(sum).isEqualTo(60L);
	}

	// --- large growth ---

	@Test
	public void testLargeGrowth() {
		ChunkedLongList list = new ChunkedLongList();
		int n = 2000;
		LongStream.range(0, n).forEach(list::add);

		Assertions.assertThat(list.size()).isEqualTo(n);
		for (int i = 0; i < n; i++) {
			Assertions.assertThat(list.getLong(i)).isEqualTo(i);
		}
	}
}
