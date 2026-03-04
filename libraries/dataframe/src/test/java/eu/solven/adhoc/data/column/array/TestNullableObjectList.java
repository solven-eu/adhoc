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
package eu.solven.adhoc.data.column.array;

import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class TestNullableObjectList {

	@Test
	public void testAddAndGet() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");
		list.add("b");

		Assertions.assertThat(list.size()).isEqualTo(2);
		Assertions.assertThat(list.get(0)).isEqualTo("a");
		Assertions.assertThat(list.get(1)).isEqualTo("b");
		Assertions.assertThat(list.isNull(0)).isFalse();
		Assertions.assertThat(list.isNull(1)).isFalse();
		Assertions.assertThat(list.sizeNotNull()).isEqualTo(2);
	}

	@Test
	public void testAddNull() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");
		list.addNull();
		list.add("c");

		Assertions.assertThat(list.size()).isEqualTo(3);
		Assertions.assertThat(list.get(0)).isEqualTo("a");
		Assertions.assertThat(list.get(1)).isNull();
		Assertions.assertThat(list.get(2)).isEqualTo("c");
		Assertions.assertThat(list.isNull(1)).isTrue();
		Assertions.assertThat(list.sizeNotNull()).isEqualTo(2);
	}

	@Test
	public void testGet_outOfBounds() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");

		// Access beyond size returns null
		Assertions.assertThat(list.get(5)).isNull();
		// Negative index returns null
		Assertions.assertThat(list.get(-1)).isNull();
	}

	@Test
	public void testSet_expandsWithGap() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		// Set at index 3 when size is 0: should create null gap at 0,1,2
		list.set(3, "d");

		Assertions.assertThat(list.size()).isEqualTo(4);
		Assertions.assertThat(list.get(0)).isNull();
		Assertions.assertThat(list.get(1)).isNull();
		Assertions.assertThat(list.get(2)).isNull();
		Assertions.assertThat(list.get(3)).isEqualTo("d");
		Assertions.assertThat(list.isNull(0)).isTrue();
		Assertions.assertThat(list.isNull(3)).isFalse();
		Assertions.assertThat(list.sizeNotNull()).isEqualTo(1);
	}

	@Test
	public void testSet_exactlyAtEnd() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");
		// Set exactly at size (appends)
		list.set(1, "b");

		Assertions.assertThat(list.size()).isEqualTo(2);
		Assertions.assertThat(list.get(1)).isEqualTo("b");
	}

	@Test
	public void testSet_overwriteNull() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");
		list.addNull();

		// Overwrite null slot at index 1 with a real value
		list.set(1, "b");

		Assertions.assertThat(list.get(1)).isEqualTo("b");
		Assertions.assertThat(list.isNull(1)).isFalse();
		Assertions.assertThat(list.sizeNotNull()).isEqualTo(2);
	}

	@Test
	public void testRemove_existingElement() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");
		list.add("b");
		list.add("c");

		// Remove marks element as null, returns old value
		String removed = list.remove(1);
		Assertions.assertThat(removed).isEqualTo("b");
		Assertions.assertThat(list.isNull(1)).isTrue();
		Assertions.assertThat(list.size()).isEqualTo(3);
	}

	@Test
	public void testRemove_alreadyNull() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");
		list.addNull();

		// Removing a null slot returns null
		String removedNull = list.remove(1);
		Assertions.assertThat(removedNull).isNull();
	}

	@Test
	public void testRemove_outOfBounds() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");

		// Remove beyond size returns null
		String removedOob = list.remove(10);
		Assertions.assertThat(removedOob).isNull();
	}

	@Test
	public void testContainsIndex() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");
		list.addNull();

		Assertions.assertThat(list.containsIndex(0)).isTrue();
		Assertions.assertThat(list.containsIndex(1)).isFalse();
		Assertions.assertThat(list.containsIndex(2)).isFalse();
	}

	@Test
	public void testIndexStream() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");
		list.addNull();
		list.add("c");

		List<Integer> indices = list.indexStream().boxed().collect(Collectors.toList());
		Assertions.assertThat(indices).containsExactly(0, 2);
	}

	@Test
	public void testForEach() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("x");
		list.addNull();
		list.add("z");

		List<String> collected = new java.util.ArrayList<>();
		list.forEach((idx, val) -> collected.add(val.toString()));

		Assertions.assertThat(collected).containsExactly("x", "z");
	}

	@Test
	public void testEmpty() {
		INullableObjectList<String> emptyList = NullableObjectList.empty();

		Assertions.assertThat(emptyList.size()).isEqualTo(0);
		Assertions.assertThat(emptyList.sizeNotNull()).isEqualTo(0);
	}

	@Test
	public void testDuplicate() {
		NullableObjectList<String> list = NullableObjectList.<String>builder().build();

		list.add("a");
		list.addNull();
		list.add("c");

		INullableObjectList<String> copy = list.duplicate();

		Assertions.assertThat(copy.size()).isEqualTo(3);
		Assertions.assertThat(copy.get(0)).isEqualTo("a");
		Assertions.assertThat(copy.get(1)).isNull();
		Assertions.assertThat(copy.get(2)).isEqualTo("c");

		// Modify original; copy must be independent
		list.set(0, "modified");
		Assertions.assertThat(copy.get(0)).isEqualTo("a");
	}
}
