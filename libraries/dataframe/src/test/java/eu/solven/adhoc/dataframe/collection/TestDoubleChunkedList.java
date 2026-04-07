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
import java.util.ConcurrentModificationException;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.collection.FrozenException;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;

public class TestDoubleChunkedList {

	static final int BASE = ChunkedArrays.BASE_DEFAULT;

	// --- constructors ---

	@Test
	public void testDefaultConstructor_empty() {
		DoubleChunkedList list = new DoubleChunkedList();

		Assertions.assertThat(list.isEmpty()).isTrue();
	}

	@Test
	public void testCapacityConstructor_zero() {
		DoubleChunkedList list = new DoubleChunkedList(0);

		Assertions.assertThat(list.isEmpty()).isTrue();
		list.add(3.14);
		Assertions.assertThat(list.getDouble(0)).isEqualTo(3.14);
	}

	@Test
	public void testCapacityConstructor_negative_throws() {
		Assertions.assertThatThrownBy(() -> new DoubleChunkedList(-1))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("-1");
	}

	@Test
	public void testCapacityConstructor_aboveBase() {
		DoubleChunkedList list = new DoubleChunkedList(300);
		IntStream.range(0, 300).forEach(i -> list.add(i * 1.0));

		Assertions.assertThat(list.size()).isEqualTo(300);
		Assertions.assertThat(list.getDouble(299)).isEqualTo(299.0);
	}

	// --- lazy head initialisation ---

	@Test
	public void testLazyInit_headIsNull() throws Exception {
		DoubleChunkedList list = new DoubleChunkedList();

		Field f = DoubleChunkedList.class.getDeclaredField("head");
		f.setAccessible(true);
		Assertions.assertThat(f.get(list)).isNull();
	}

	@Test
	public void testLazyInit_headAllocatedOnFirstAdd() throws Exception {
		DoubleChunkedList list = new DoubleChunkedList();
		Field f = DoubleChunkedList.class.getDeclaredField("head");
		f.setAccessible(true);

		Assertions.assertThat(f.get(list)).isNull();
		list.add(1.0);
		Assertions.assertThat(f.get(list)).isNotNull();
	}

	// --- add / getDouble ---

	@Test
	public void testAdd_andGet_sequential() {
		DoubleChunkedList list = new DoubleChunkedList();
		for (int i = 0; i < 100; i++) {
			list.add(i * 0.5);
		}

		Assertions.assertThat(list.size()).isEqualTo(100);
		for (int i = 0; i < 100; i++) {
			Assertions.assertThat(list.getDouble(i)).isEqualTo(i * 0.5);
		}
	}

	@Test
	public void testAdd_specialValues() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(0.0);
		list.add(-0.0);
		list.add(Double.NaN);
		list.add(Double.POSITIVE_INFINITY);
		list.add(Double.NEGATIVE_INFINITY);
		list.add(Double.MIN_VALUE);
		list.add(Double.MAX_VALUE);

		Assertions.assertThat(list.getDouble(0)).isEqualTo(0.0);
		Assertions.assertThat(list.getDouble(2)).isNaN();
		Assertions.assertThat(list.getDouble(3)).isEqualTo(Double.POSITIVE_INFINITY);
		Assertions.assertThat(list.getDouble(4)).isEqualTo(Double.NEGATIVE_INFINITY);
	}

	@Test
	public void testGet_outOfBounds_throws() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);

		Assertions.assertThatThrownBy(() -> list.getDouble(1)).isInstanceOf(IndexOutOfBoundsException.class);
		Assertions.assertThatThrownBy(() -> list.getDouble(-1)).isInstanceOf(IndexOutOfBoundsException.class);
	}

	// --- head/tail boundary ---

	@Test
	public void testHeadBoundary_lastHeadAndFirstTail() {
		DoubleChunkedList list = new DoubleChunkedList();
		IntStream.range(0, BASE + 1).forEach(i -> list.add(i * 1.0));

		Assertions.assertThat(list.getDouble(BASE - 1)).isEqualTo((BASE - 1) * 1.0);
		Assertions.assertThat(list.getDouble(BASE)).isEqualTo(BASE * 1.0);
	}

	@Test
	public void testTailChunkBoundaries() {
		DoubleChunkedList list = new DoubleChunkedList();
		IntStream.range(0, 1024).forEach(i -> list.add(i * 1.0));

		int[] boundaries = { 0, 127, 128, 255, 256, 511, 512, 1023 };
		for (int i : boundaries) {
			Assertions.assertThat(list.getDouble(i)).as("index %d", i).isEqualTo(i * 1.0);
		}
	}

	// --- set ---

	@Test
	public void testSet_returnsOldValue() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.add(2.0);
		list.add(3.0);

		double old = list.set(1, 99.0);

		Assertions.assertThat(old).isEqualTo(2.0);
		Assertions.assertThat(list.getDouble(1)).isEqualTo(99.0);
		Assertions.assertThat(list.size()).isEqualTo(3);
	}

	@Test
	public void testSet_inTail() {
		DoubleChunkedList list = new DoubleChunkedList();
		IntStream.range(0, 300).forEach(i -> list.add(i * 1.0));

		list.set(200, 999.5);

		Assertions.assertThat(list.getDouble(200)).isEqualTo(999.5);
		Assertions.assertThat(list.getDouble(199)).isEqualTo(199.0);
		Assertions.assertThat(list.getDouble(201)).isEqualTo(201.0);
	}

	// --- add at index / remove ---

	@Test
	public void testAddAtIndex_shiftsRight() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(0.0);
		list.add(1.0);
		list.add(3.0);

		list.add(2, 2.0);

		Assertions.assertThat(list.toDoubleArray()).containsExactly(0.0, 1.0, 2.0, 3.0);
	}

	@Test
	public void testRemove_shiftsLeft() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.add(2.0);
		list.add(3.0);

		double removed = list.removeDouble(1);

		Assertions.assertThat(removed).isEqualTo(2.0);
		Assertions.assertThat(list.toDoubleArray()).containsExactly(1.0, 3.0);
	}

	@Test
	public void testRemove_outOfBounds_throws() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);

		Assertions.assertThatThrownBy(() -> list.removeDouble(1)).isInstanceOf(IndexOutOfBoundsException.class);
	}

	// --- clear ---

	@Test
	public void testClear_headOnly() {
		DoubleChunkedList list = new DoubleChunkedList();
		IntStream.range(0, 50).forEach(i -> list.add(i * 1.0));
		list.clear();

		Assertions.assertThat(list.isEmpty()).isTrue();
		// can still add after clear
		list.add(7.0);
		Assertions.assertThat(list.getDouble(0)).isEqualTo(7.0);
	}

	@Test
	public void testClear_withTail() {
		DoubleChunkedList list = new DoubleChunkedList();
		IntStream.range(0, 500).forEach(i -> list.add(i * 1.0));
		list.clear();

		Assertions.assertThat(list.isEmpty()).isTrue();
	}

	// --- compact ---

	@Test
	public void testIsFrozen_falseBeforeCompact() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);

		Assertions.assertThat(list.isFrozen()).isFalse();
	}

	@Test
	public void testIsFrozen_trueAfterCompact() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.compact();

		Assertions.assertThat(list.isFrozen()).isTrue();
	}

	@Test
	public void testCompact_emptyList_noNpe() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.compact();

		Assertions.assertThat(list.isEmpty()).isTrue();
		Assertions.assertThat(list.isFrozen()).isTrue();
	}

	@Test
	public void testCompact_trimsHead_twoElements() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.5);
		list.add(2.5);
		list.compact();

		Assertions.assertThat(list.size()).isEqualTo(2);
		Assertions.assertThat(list.getDouble(0)).isEqualTo(1.5);
		Assertions.assertThat(list.getDouble(1)).isEqualTo(2.5);
	}

	@Test
	public void testCompact_headOnly_readsCorrectly() {
		DoubleChunkedList list = new DoubleChunkedList();
		IntStream.range(0, 50).forEach(i -> list.add(i * 1.0));
		list.compact();

		Assertions.assertThat(list.size()).isEqualTo(50);
		for (int i = 0; i < 50; i++) {
			Assertions.assertThat(list.getDouble(i)).isEqualTo(i * 1.0);
		}
	}

	@Test
	public void testCompact_withTail_readsCorrectly() {
		DoubleChunkedList list = new DoubleChunkedList();
		IntStream.range(0, 300).forEach(i -> list.add(i * 1.0));
		list.compact();

		Assertions.assertThat(list.size()).isEqualTo(300);
		for (int i = 0; i < 300; i++) {
			Assertions.assertThat(list.getDouble(i)).isEqualTo(i * 1.0);
		}
	}

	@Test
	public void testCompact_preventsAdd() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.compact();

		Assertions.assertThatThrownBy(() -> list.add(2.0)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsAddAtIndex() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.compact();

		Assertions.assertThatThrownBy(() -> list.add(0, 2.0)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsSet() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.compact();

		Assertions.assertThatThrownBy(() -> list.set(0, 99.0)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsRemove() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.compact();

		Assertions.assertThatThrownBy(() -> list.removeDouble(0)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsClear() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.compact();

		Assertions.assertThatThrownBy(list::clear).isInstanceOf(FrozenException.class);
	}

	// --- iterator / CME ---

	@Test
	public void testIterator_traversesAllElements() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.add(2.0);
		list.add(3.0);

		double sum = 0.0;
		DoubleIterator it = list.iterator();
		while (it.hasNext()) {
			sum += it.nextDouble();
		}

		Assertions.assertThat(sum).isEqualTo(6.0);
	}

	@Test
	public void testIterator_cme_onAdd() {
		DoubleChunkedList list = new DoubleChunkedList();
		list.add(1.0);
		list.add(2.0);
		DoubleIterator it = list.iterator();
		it.nextDouble();
		list.add(3.0);

		Assertions.assertThatThrownBy(it::nextDouble).isInstanceOf(ConcurrentModificationException.class);
	}

	// --- large growth ---

	@Test
	public void testLargeGrowth() {
		DoubleChunkedList list = new DoubleChunkedList();
		int n = 2000;
		IntStream.range(0, n).forEach(i -> list.add(i * 1.0));

		Assertions.assertThat(list.size()).isEqualTo(n);
		for (int i = 0; i < n; i++) {
			Assertions.assertThat(list.getDouble(i)).isEqualTo(i * 1.0);
		}
	}
}
