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

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMultitypeArrayColumn {
	MultitypeArrayColumn<Integer> column = MultitypeArrayColumn.<Integer>builder().build();

	Integer k1 = 17;
	Integer k2 = 21;

	@Test
	public void testIntAndInt() {
		column.set(k1, 123);
		column.set(k1, 234);

		column.onValue(k1, o -> {
			Assertions.assertThat(o).isEqualTo(234L);
		});
	}

	@Test
	public void testIntAndLong() {
		column.set(k1, 123);
		column.set(k1, 234L);

		column.onValue(k1, o -> {
			Assertions.assertThat(o).isEqualTo(234L);
		});
	}

	@Test
	public void testIntAndDouble() {
		column.set(k1, 123);
		column.set(k1, 23.45D);

		column.onValue(k1, o -> {
			Assertions.assertThat(o).isEqualTo(23.45D);
		});
	}

	@Test
	public void testIntAndString() {
		column.set(k1, 123);
		column.set(k1, "234");
		column.set(k1, 345);

		column.onValue(k1, o -> {
			Assertions.assertThat(o).isEqualTo(345L);
		});
	}

	@Test
	public void testIntAndNull() {
		column.set(k1, 123);
		column.set(k1, null);
		column.set(k1, 345);

		column.onValue(k1, o -> {
			Assertions.assertThat(o).isEqualTo(0L + 345);
		});
	}

	@Test
	public void testStringAndLocalDate() {
		LocalDate today = LocalDate.now();

		column.set(k1, "foo");
		column.set(k1, today);

		column.onValue(k1, o -> {
			Assertions.assertThat(o).isEqualTo(today);
		});
	}

	@Test
	public void testStringAndString() {
		column.set(k1, "123");
		column.set(k1, "234");

		column.onValue(k1, o -> {
			Assertions.assertThat(o).isEqualTo("234");
		});
	}

	@Test
	public void testClearKey() {
		column.set(k1, 123);
		column.clearKey(k1);

		Assertions.assertThat(column.size()).isEqualTo(0);
	}

	@Test
	public void testAppendNullOnInt() {
		column.set(k1, 123);

		// TODO We may accept `null`, applying a `remove`
		Assertions.assertThatThrownBy(() -> column.append(k1)).isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	public void testToString() {
		LocalDate today = LocalDate.now();

		column.set(k1).onObject(today);
		column.set(k2).onLong(123);

		// The order of types is fixed, and the order within each type is guaranteed
		// This structure is not sorted
		Assertions.assertThat(column.keyStream().toList()).containsExactly(k2, k1);

		Assertions.assertThat(column.toString())
				.isEqualTo(
						"MultitypeArrayColumn{#longs=1, #objects=1, #0-21=123(java.lang.Long), #1-17=%s(java.time.LocalDate)}"
								.formatted(today));
	}

	@Test
	public void testStream() {
		LocalDate today = LocalDate.now();

		column.set(k1).onObject(today);
		column.set(k2).onLong(123);

		List<Entry<Integer, Object>> listOfEntry = column.stream(slice -> v -> Map.entry(slice, v)).toList();

		Assertions.assertThat(listOfEntry).hasSize(2).contains(Map.entry(k1, today)).contains(Map.entry(k2, 123L));
	}

	@Test
	public void testNull() {
		column.append(k1).onObject(null);

		column.onValue(k1, o -> {
			Assertions.assertThat(o).isNull();
		});

		Assertions.assertThat(column.toString()).isEqualTo("MultitypeArrayColumn{}");
	}

	@Test
	public void testAppend() {
		column.append(3).onObject("foo");
		column.append(1).onLong(123);

		Assertions.assertThat(column.stream(i -> o -> o).toList()).containsExactly(123L, "foo");
	}

	@Test
	public void testCompact() {
		// Compact while empty
		column.compact();

		column.append(1).onObject("foo");
		column.append(2).onLong(123);
		column.append(3).onDouble(12.34D);

		// Compact while non-empty
		column.compact();
	}
}
