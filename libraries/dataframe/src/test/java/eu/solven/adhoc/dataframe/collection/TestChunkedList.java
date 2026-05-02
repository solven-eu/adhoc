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
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.collection.FrozenException;

public class TestChunkedList {

	// --- tailChunkIndex arithmetic ---

	@Test
	public void testTailChunkIndex() {
		// tailChunkIndex(unitIndex) = floor(log2(unitIndex + 1))
		Assertions.assertThat(ChunkedList.tailChunkIndex(0)).isEqualTo(0);
		Assertions.assertThat(ChunkedList.tailChunkIndex(1)).isEqualTo(1);
		Assertions.assertThat(ChunkedList.tailChunkIndex(2)).isEqualTo(1);
		Assertions.assertThat(ChunkedList.tailChunkIndex(3)).isEqualTo(2);
		Assertions.assertThat(ChunkedList.tailChunkIndex(6)).isEqualTo(2);
		Assertions.assertThat(ChunkedList.tailChunkIndex(7)).isEqualTo(3);
	}

	@Test
	public void computeLog2Base() {
		// If the entry is small, we prefer initializing 128 entries anyway
		Assertions.assertThat(ChunkedList.computeLog2Base(0)).isEqualTo(7);
		Assertions.assertThat(ChunkedList.computeLog2Base(7)).isEqualTo(7);

		Assertions.assertThat(ChunkedList.computeLog2Base(128)).isEqualTo(7);
		Assertions.assertThat(ChunkedList.computeLog2Base(128 + 1)).isEqualTo(6);
		Assertions.assertThat(ChunkedList.computeLog2Base(128 + 64)).isEqualTo(6);
		Assertions.assertThat(ChunkedList.computeLog2Base(128 + 64 + 1)).isEqualTo(8);

		Assertions.assertThat(ChunkedList.computeLog2Base(256 - 1)).isEqualTo(8);
		Assertions.assertThat(ChunkedList.computeLog2Base(256)).isEqualTo(8);
		Assertions.assertThat(ChunkedList.computeLog2Base(256 + 1)).isEqualTo(7);

		// Given 128+256 initial capacity, we prefer initializing a base with 128 entries and a first chunk with 256
		Assertions.assertThat(ChunkedList.computeLog2Base(256 + 128)).isEqualTo(7);
		Assertions.assertThat(ChunkedList.computeLog2Base(256 + 128 + 1)).isEqualTo(9);
		Assertions.assertThat(ChunkedList.computeLog2Base(511)).isEqualTo(9);
		Assertions.assertThat(ChunkedList.computeLog2Base(512)).isEqualTo(9);
		Assertions.assertThat(ChunkedList.computeLog2Base(512 + 1)).isEqualTo(8);
	}

	// --- head / tail boundary ---

	@Test
	public void testHeadBoundary_lastHeadAndFirstTail() {
		ChunkedList<Integer> list = new ChunkedList<>();
		IntStream.range(0, ChunkedList.BASE_DEFAULT + 1).forEach(list::add);

		// last element in head
		Assertions.assertThat(list.get(ChunkedList.BASE_DEFAULT - 1)).isEqualTo(ChunkedList.BASE_DEFAULT - 1);
		// first element in tail
		Assertions.assertThat(list.get(ChunkedList.BASE_DEFAULT)).isEqualTo(ChunkedList.BASE_DEFAULT);
	}

	@Test
	public void testTailChunkBoundaries() {
		// Layout: head=128, tail[0]=128(128-255), tail[1]=256(256-511), tail[2]=512(512-1023)
		int capacity = 1024;
		ChunkedList<Integer> list = new ChunkedList<>();
		IntStream.range(0, capacity).forEach(list::add);

		int[] boundaries = { 0, 127, 128, 255, 256, 511, 512, 1023 };
		for (int i : boundaries) {
			Assertions.assertThat(list.get(i)).as("index %d", i).isEqualTo(i);
		}
	}

	// --- basic add / get ---

	@Test
	public void testAdd_andGet_sequential() {
		ChunkedList<Integer> list = new ChunkedList<>();
		for (int i = 0; i < 100; i++) {
			list.add(i);
		}
		Assertions.assertThat((List) list).hasSize(100);
		for (int i = 0; i < 100; i++) {
			Assertions.assertThat(list.get(i)).isEqualTo(i);
		}
	}

	// --- set ---

	@Test
	public void testSet_returnsOldValue() {
		ChunkedList<String> list = new ChunkedList<>();
		list.add("a");
		list.add("b");
		list.add("c");

		String old = list.set(1, "B");

		Assertions.assertThat(old).isEqualTo("b");
		Assertions.assertThat(list.get(1)).isEqualTo("B");
		Assertions.assertThat((List) list).hasSize(3);
	}

	@Test
	public void testSet_inTail() {
		ChunkedList<Integer> list = new ChunkedList<>();
		IntStream.range(0, 300).forEach(list::add);

		list.set(200, 999);

		Assertions.assertThat(list.get(200)).isEqualTo(999);
		Assertions.assertThat(list.get(199)).isEqualTo(199);
		Assertions.assertThat(list.get(201)).isEqualTo(201);
	}

	// --- add at index / remove ---

	@Test
	public void testAddAtIndex_shiftsRight() {
		ChunkedList<Integer> list = new ChunkedList<>();
		list.add(0);
		list.add(1);
		list.add(3);

		list.add(2, 2);

		Assertions.assertThat((List) list).containsExactly(0, 1, 2, 3);
	}

	@Test
	public void testRemove_shiftsLeft() {
		ChunkedList<String> list = new ChunkedList<>();
		list.add("a");
		list.add("b");
		list.add("c");

		String removed = list.remove(1);

		Assertions.assertThat(removed).isEqualTo("b");
		Assertions.assertThat((List) list).containsExactly("a", "c");
	}

	// --- clear ---

	@Test
	public void testClear_headOnly() {
		ChunkedList<Integer> list = new ChunkedList<>();
		IntStream.range(0, 50).forEach(list::add);
		list.clear();

		Assertions.assertThat((List) list).isEmpty();
	}

	@Test
	public void testClear_withTail() {
		ChunkedList<Integer> list = new ChunkedList<>();
		IntStream.range(0, 500).forEach(list::add);
		list.clear();

		Assertions.assertThat((List) list).isEmpty();
	}

	// --- constructors ---

	@Test
	public void testConstructor_fromCollection() {
		List<Integer> source = List.of(10, 20, 30, 40, 50);
		ChunkedList<Integer> list = new ChunkedList<>(source);

		Assertions.assertThat((List) list).containsExactlyElementsOf(source);
	}

	@Test
	public void testConstructor_withInitialCapacity_aboveBase() {
		ChunkedList<String> list = new ChunkedList<>(300);
		for (int i = 0; i < 300; i++) {
			list.add("x" + i);
		}
		Assertions.assertThat((List) list).hasSize(300);
		Assertions.assertThat(list.get(299)).isEqualTo("x299");
	}

	@Test
	public void testConstructor_zeroCapacity() {
		ChunkedList<String> list = new ChunkedList<>(0);
		list.add("first");

		Assertions.assertThat((List) list).containsExactly("first");
	}

	// --- customizable base ---

	@Test
	public void testCustomBase_powerOfTwo_fitsInHead() {
		// capacity=256 → base=256; all 256 elements stay in head with no tail
		ChunkedList<Integer> list = new ChunkedList<>(256);
		IntStream.range(0, 256).forEach(list::add);

		Assertions.assertThat((List) list).hasSize(256);
		Assertions.assertThat(list.get(0)).isEqualTo(0);
		Assertions.assertThat(list.get(255)).isEqualTo(255);
	}

	@Test
	public void testCustomBase_overflow_usesCorrectTail() {
		// capacity=256 → base=256; adding 257 elements forces a single tail chunk
		ChunkedList<Integer> list = new ChunkedList<>(256);
		IntStream.range(0, 257).forEach(list::add);

		Assertions.assertThat((List) list).hasSize(257);
		Assertions.assertThat(list.get(255)).isEqualTo(255);
		Assertions.assertThat(list.get(256)).isEqualTo(256);
	}

	@Test
	public void testCustomBase_nonPowerOfTwo_keepsDefaultBase() {
		// capacity=300: 300/512=58% < 75% threshold → base stays at 128; elements spill into tail chunks
		ChunkedList<Integer> list = new ChunkedList<>(300);
		IntStream.range(0, 300).forEach(list::add);

		Assertions.assertThat((List) list).hasSize(300);
		IntStream.range(0, 300).forEach(i -> Assertions.assertThat(list.get(i)).isEqualTo(i));
	}

	@Test
	public void testCustomBase_nearNextPowerOfTwo_promotesBase() {
		// capacity=510: 510/512=99% >= 75% threshold → base=512; all elements fit in head
		ChunkedList<Integer> list = new ChunkedList<>(510);
		IntStream.range(0, 510).forEach(list::add);

		Assertions.assertThat((List) list).hasSize(510);
		Assertions.assertThat(list.get(0)).isEqualTo(0);
		Assertions.assertThat(list.get(509)).isEqualTo(509);
	}

	// --- null elements ---

	@Test
	public void testNullElements() {
		ChunkedList<String> list = new ChunkedList<>();
		list.add(null);
		list.add("a");
		list.add(null);

		Assertions.assertThat(list.get(0)).isNull();
		Assertions.assertThat(list.get(1)).isEqualTo("a");
		Assertions.assertThat(list.get(2)).isNull();
	}

	// --- empty list ---

	@Test
	public void testEmptyList() {
		ChunkedList<String> list = new ChunkedList<>();

		Assertions.assertThat((List) list).isEmpty();
		Assertions.assertThat(list.size()).isZero();
		Assertions.assertThat(list.contains("x")).isFalse();
		Assertions.assertThat(list.indexOf("x")).isEqualTo(-1);
	}

	// --- single element ---

	@Test
	public void testSingleElement_addGetRemove() {
		ChunkedList<String> list = new ChunkedList<>();
		list.add("only");

		Assertions.assertThat(list.get(0)).isEqualTo("only");
		Assertions.assertThat(list.remove(0)).isEqualTo("only");
		Assertions.assertThat((List) list).isEmpty();
	}

	// --- add at head and tail ---

	@Test
	public void testAddAtHead_repeatedlyShiftsAll() {
		ChunkedList<Integer> list = new ChunkedList<>();
		for (int i = 4; i >= 0; i--) {
			list.add(0, i);
		}

		Assertions.assertThat((List) list).containsExactly(0, 1, 2, 3, 4);
	}

	@Test
	public void testRemoveHead_repeatedlyShifts() {
		ChunkedList<Integer> list = new ChunkedList<>(List.of(0, 1, 2, 3, 4));

		for (int i = 0; i < 5; i++) {
			Assertions.assertThat(list.remove(0)).isEqualTo(i);
		}
		Assertions.assertThat((List) list).isEmpty();
	}

	// --- contains / indexOf / lastIndexOf ---

	@Test
	public void testContains_andIndexOf() {
		ChunkedList<String> list = new ChunkedList<>(List.of("a", "b", "c", "b"));

		Assertions.assertThat(list.contains("b")).isTrue();
		Assertions.assertThat(list.contains("z")).isFalse();
		Assertions.assertThat(list.indexOf("b")).isEqualTo(1);
		Assertions.assertThat(list.lastIndexOf("b")).isEqualTo(3);
		Assertions.assertThat(list.indexOf("z")).isEqualTo(-1);
	}

	// --- addAll ---

	@Test
	public void testAddAll_appendsCollection() {
		ChunkedList<Integer> list = new ChunkedList<>();
		list.add(0);
		list.addAll(List.of(1, 2, 3));

		Assertions.assertThat((List) list).containsExactly(0, 1, 2, 3);
	}

	@Test
	public void testAddAll_atIndex_insertsInMiddle() {
		ChunkedList<Integer> list = new ChunkedList<>(List.of(0, 3, 4));
		list.addAll(1, List.of(1, 2));

		Assertions.assertThat((List) list).containsExactly(0, 1, 2, 3, 4);
	}

	// --- equals and hashCode (from AbstractList) ---

	@Test
	public void testEquals_matchingArrayList() {
		List<Integer> reference = new ArrayList<>(List.of(1, 2, 3));
		ChunkedList<Integer> list = new ChunkedList<>(reference);

		Assertions.assertThat((List) list).isEqualTo(reference);
		Assertions.assertThat(list.hashCode()).isEqualTo(reference.hashCode());
	}

	@Test
	public void testEquals_emptyLists() {
		Assertions.assertThat((List) new ChunkedList<>()).isEqualTo(new ArrayList<>());
	}

	// --- bounds checks ---

	@Test
	public void testGet_outOfBounds_throws() {
		ChunkedList<String> list = new ChunkedList<>();
		list.add("a");

		Assertions.assertThatThrownBy(() -> list.get(1)).isInstanceOf(IndexOutOfBoundsException.class);
		Assertions.assertThatThrownBy(() -> list.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
	}

	// --- iterator and ConcurrentModificationException ---

	@Test
	public void testIterator_traversesAllElements() {
		ChunkedList<Integer> list = new ChunkedList<>(List.of(10, 20, 30));
		List<Integer> collected = new ArrayList<>();
		for (Integer v : list) {
			collected.add(v);
		}

		Assertions.assertThat(collected).containsExactly(10, 20, 30);
	}

	@Test
	public void testListIterator_bidirectional() {
		ChunkedList<String> list = new ChunkedList<>(List.of("a", "b", "c"));
		ListIterator<String> it = list.listIterator(3);

		List<String> reverse = new ArrayList<>();
		while (it.hasPrevious()) {
			reverse.add(it.previous());
		}

		Assertions.assertThat(reverse).containsExactly("c", "b", "a");
	}

	// --- subList ---

	@Test
	public void testSubList_readsCorrectly() {
		ChunkedList<Integer> list = new ChunkedList<>();
		IntStream.range(0, 10).forEach(list::add);

		Assertions.assertThat((List) list.subList(3, 7)).containsExactly(3, 4, 5, 6);
	}

	// --- compact ---

	@Test
	public void testIsFrozen_falseBeforeCompact() {
		ChunkedList<String> list = new ChunkedList<>();
		list.add("a");

		Assertions.assertThat(list.isFrozen()).isFalse();
	}

	@Test
	public void testIsFrozen_trueAfterCompact() {
		ChunkedList<String> list = new ChunkedList<>();
		list.add("a");
		list.compact();

		Assertions.assertThat(list.isFrozen()).isTrue();
	}

	@Test
	public void testCompact_trimsHead_twoElements() {
		// A 2-element list has a head of 128 slots; compact() must shrink it to 2.
		ChunkedList<String> list = new ChunkedList<>();
		list.add("x");
		list.add("y");
		list.compact();

		Assertions.assertThat((List) list).hasSize(2);
		Assertions.assertThat(list.get(0)).isEqualTo("x");
		Assertions.assertThat(list.get(1)).isEqualTo("y");
	}

	@Test
	public void testCompact_headOnly_readsCorrectly() {
		ChunkedList<Integer> list = new ChunkedList<>();
		IntStream.range(0, 50).forEach(list::add);
		list.compact();

		Assertions.assertThat((List) list).hasSize(50);
		IntStream.range(0, 50).forEach(i -> Assertions.assertThat(list.get(i)).isEqualTo(i));
	}

	@Test
	public void testCompact_withTail_readsCorrectly() {
		ChunkedList<Integer> list = new ChunkedList<>();
		IntStream.range(0, 300).forEach(list::add);
		list.compact();

		Assertions.assertThat((List) list).hasSize(300);
		IntStream.range(0, 300).forEach(i -> Assertions.assertThat(list.get(i)).isEqualTo(i));
	}

	@Test
	public void testCompact_preventsAdd() {
		ChunkedList<Integer> list = new ChunkedList<>(List.of(1, 2, 3));
		list.compact();

		Assertions.assertThatThrownBy(() -> list.add(4)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsSet() {
		ChunkedList<Integer> list = new ChunkedList<>(List.of(1, 2, 3));
		list.compact();

		Assertions.assertThatThrownBy(() -> list.set(0, 99)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsRemove() {
		ChunkedList<Integer> list = new ChunkedList<>(List.of(1, 2, 3));
		list.compact();

		Assertions.assertThatThrownBy(() -> list.remove(0)).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_preventsClear() {
		ChunkedList<Integer> list = new ChunkedList<>(List.of(1, 2, 3));
		list.compact();

		Assertions.assertThatThrownBy(list::clear).isInstanceOf(FrozenException.class);
	}

	@Test
	public void testCompact_trimsLastChunk() {
		// Fill exactly 200 elements: head=128, tail[0] partially filled (72 of 128 slots used)
		ChunkedList<Integer> list = new ChunkedList<>();
		IntStream.range(0, 200).forEach(list::add);
		list.compact();

		// After compact, reads must still be correct
		Assertions.assertThat(list.get(127)).isEqualTo(127);
		Assertions.assertThat(list.get(128)).isEqualTo(128);
		Assertions.assertThat(list.get(199)).isEqualTo(199);
	}

	// --- lazy head initialisation ---

	@Test
	public void testLazyInit_defaultConstructor_headIsNull() throws Exception {
		ChunkedList<String> list = new ChunkedList<>();

		Field f = ChunkedList.class.getDeclaredField("head");
		f.setAccessible(true);
		Assertions.assertThat(f.get(list)).isNull();
	}

	@Test
	public void testLazyInit_capacityConstructor_headIsNull() throws Exception {
		ChunkedList<String> list = new ChunkedList<>(0);

		Field f = ChunkedList.class.getDeclaredField("head");
		f.setAccessible(true);
		Assertions.assertThat(f.get(list)).isNull();
	}

	@Test
	public void testLazyInit_headAllocatedOnFirstAdd() throws Exception {
		ChunkedList<String> list = new ChunkedList<>();
		Field f = ChunkedList.class.getDeclaredField("head");
		f.setAccessible(true);

		Assertions.assertThat(f.get(list)).isNull();
		list.add("first");
		Assertions.assertThat(f.get(list)).isNotNull();
	}

	@Test
	public void testLazyInit_compact_emptyList_noNpe() {
		ChunkedList<String> list = new ChunkedList<>();
		// compact() on an unwritten list must not throw NullPointerException
		list.compact();

		Assertions.assertThat((List) list).isEmpty();
		Assertions.assertThat(list.isFrozen()).isTrue();
	}

	@Test
	public void testLazyInit_clear_emptyList_noNpe() {
		ChunkedList<String> list = new ChunkedList<>();
		// clear() on an unwritten list must not throw NullPointerException
		list.clear();

		Assertions.assertThat((List) list).isEmpty();
	}

	// --- large growth ---

	@Test
	public void testLargeGrowth() {
		ChunkedList<Integer> list = new ChunkedList<>();
		int n = 2000;
		IntStream.range(0, n).forEach(list::add);

		Assertions.assertThat((List) list).hasSize(n);
		IntStream.range(0, n).forEach(i -> Assertions.assertThat(list.get(i)).isEqualTo(i));
	}
}
