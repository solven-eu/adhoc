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
package eu.solven.adhoc.map;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.map.AdhocMap.AdhocMapBuilder;

public class TestAdhocMap {
	// Not a String to ensure we accept various types
	LocalDate now = LocalDate.now();

	@Test
	public void testGet() {
		IAdhocMap simpleMap = AdhocMap.builder(ImmutableSet.of("a", "b")).append("a1").append(now).build();

		Assertions.assertThat((Map) simpleMap).containsEntry("a", "a1").containsEntry("b", now).hasSize(2);

		Assertions.assertThat(simpleMap.containsKey("a")).isTrue();
		Assertions.assertThat(simpleMap.get("a")).isEqualTo("a1");

		Assertions.assertThat(simpleMap.containsKey("b")).isTrue();
		Assertions.assertThat(simpleMap.get("b")).isEqualTo(now);

		Assertions.assertThat(simpleMap.containsKey("c")).isFalse();
		Assertions.assertThat(simpleMap.get("c")).isEqualTo(null);
	}

	@Test
	public void testHashcode() {
		IAdhocMap simpleMap = AdhocMap.builder(ImmutableSet.of("a", "b")).append("a1").append(now).build();

		{
			IAdhocMap simpleMap2 = AdhocMap.builder(ImmutableSet.of("a", "b")).append("a1").append(now).build();

			Assertions.assertThat((Map) simpleMap).isEqualTo(simpleMap2);
		}

		// Same keys different values
		{
			IAdhocMap simpleMap2 =
					AdhocMap.builder(ImmutableSet.of("a", "b")).append("a1").append(now.plusDays(1)).build();

			Assertions.assertThat((Map) simpleMap).isNotEqualTo(simpleMap2);
		}

		// Same values different keys
		{
			IAdhocMap simpleMap2 = AdhocMap.builder(ImmutableSet.of("a", "c")).append("a1").append(now).build();

			Assertions.assertThat((Map) simpleMap).isNotEqualTo(simpleMap2);
		}

		// Equals with standard map
		{
			Map<String, Object> asStandardMap = Map.of("a", "a1", "b", now);
			Assertions.assertThat((Map) simpleMap).isEqualTo(asStandardMap);
			Assertions.assertThat(asStandardMap).isEqualTo(simpleMap);
			Assertions.assertThat(simpleMap.hashCode()).isEqualTo(asStandardMap.hashCode());

			Assertions.assertThat(simpleMap.entrySet()).isEqualTo(asStandardMap.entrySet());
		}
	}

	@Test
	public void testReOrder_2() {
		IAdhocMap asc = AdhocMap.builder(ImmutableSet.of("a", "date")).append("a1").append(now).build();
		IAdhocMap desc = AdhocMap.builder(ImmutableSet.of("date", "a")).append(now).append("a1").build();

		Assertions.assertThat((Map) desc).isEqualTo(asc).isEqualTo(Map.of("a", "a1", "date", now));
	}

	@Test
	public void testReOrder_3_mixed() {
		IAdhocMap asc = AdhocMap.builder(ImmutableSet.of("a", "c", "b")).append("a1").append("c1").append("b1").build();
		IAdhocMap desc =
				AdhocMap.builder(ImmutableSet.of("a", "b", "c")).append("a1").append("b1").append("c1").build();

		Assertions.assertThat((Map) desc).isEqualTo(asc).isEqualTo(Map.of("a", "a1", "b", "b1", "c", "c1"));
	}

	@Test
	public void testReOrder_3_mixed2() {
		IAdhocMap asc = AdhocMap.builder(ImmutableSet.of("c", "a", "b")).append("c1").append("a1").append("b1").build();
		IAdhocMap desc =
				AdhocMap.builder(ImmutableSet.of("a", "b", "c")).append("a1").append("b1").append("c1").build();

		Assertions.assertThat((Map) desc).isEqualTo(asc).isEqualTo(Map.of("a", "a1", "b", "b1", "c", "c1"));
	}

	@Test
	public void testReOrder_3_reverse() {
		IAdhocMap asc = AdhocMap.builder(ImmutableSet.of("c", "b", "a")).append("c1").append("b1").append("a1").build();
		IAdhocMap desc =
				AdhocMap.builder(ImmutableSet.of("a", "b", "c")).append("a1").append("b1").append("c1").build();

		Assertions.assertThat((Map) desc).isEqualTo(asc).isEqualTo(Map.of("a", "a1", "b", "b1", "c", "c1"));
	}

	@Test
	public void testCompare() {
		AdhocMap b1Now = AdhocMap.builder(ImmutableSet.of("b", "date")).append("b1").append(now).build();
		AdhocMap b1Tomorrow =
				AdhocMap.builder(ImmutableSet.of("b", "date")).append("b1").append(now.plusDays(1)).build();

		Assertions.assertThatComparable(b1Now).isLessThan(b1Tomorrow);

		AdhocMap a1Now = AdhocMap.builder(ImmutableSet.of("a", "date")).append("a1").append(now).build();
		Assertions.assertThatComparable(a1Now).isLessThan(b1Now);

		AdhocMap a1b1Now =
				AdhocMap.builder(ImmutableSet.of("a", "b", "date")).append("a1").append("b1").append(now).build();
		Assertions.assertThatComparable(a1b1Now)
				// `b` is less than `date`
				.isLessThanOrEqualTo(a1Now)
				// Leading `a` before common `b` make it smaller
				.isLessThanOrEqualTo(b1Now);

		AdhocMap b1NowZ1 =
				AdhocMap.builder(ImmutableSet.of("b", "date", "z")).append("b1").append(now).append("z1").build();
		Assertions.assertThatComparable(b1NowZ1)
				// Trailing `z` after common `date` make is greater
				.isGreaterThan(b1Now);
	}

	@Test
	public void testEntrySet_Order() {
		int size = 128;
		ImmutableSet<String> keys = IntStream.range(0, size)
				.mapToObj(i -> Strings.padStart(Integer.toString(i), 3, '0'))
				.collect(ImmutableSet.toImmutableSet());

		AdhocMapBuilder builder = AdhocMap.builder(keys);

		IntStream.range(0, size).forEach(i -> builder.append("v_" + i));

		AdhocMap map = builder.build();

		List<Entry<String, Object>> entries = map.entrySet().stream().toList();
		for (int i = 0; i < size; i++) {
			Assertions.assertThat(entries.get(i))
					.as("i=%s", i)
					.isEqualTo(Map.entry(Strings.padStart(Integer.toString(i), 3, '0'), "v_" + i));
		}
	}
}
