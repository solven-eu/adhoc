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
package eu.solven.adhoc.map.factory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.compression.ColumnarSliceFactory;
import eu.solven.adhoc.map.IAdhocMap;

public class TestColumnarSliceFactory {
	ColumnarSliceFactory factory = ColumnarSliceFactory.builder().build();

	@Test
	public void isNorOrdered() {
		Assertions.assertThat(factory.isNotOrdered(Set.of("a"))).isTrue();

		{
			HashSet<String> hashSet = new HashSet<>();
			Assertions.assertThat(factory.isNotOrdered(hashSet)).isFalse();
		}

		// HashMap keySet is kind-of ordered, in the sense we expect to iterate in `.values` with same ordering as
		// `.keySet`
		{
			HashMap<String, Object> hashMap = new HashMap<>();
			Assertions.assertThat(factory.isNotOrdered(hashMap.keySet())).isFalse();
		}

		// Empty so ordered
		Assertions.assertThat(factory.isNotOrdered(Set.of())).isFalse();
		// These implementations are ordered
		Assertions.assertThat(factory.isNotOrdered(List.of())).isFalse();
		Assertions.assertThat(factory.isNotOrdered(ImmutableList.of())).isFalse();
		Assertions.assertThat(factory.isNotOrdered(ImmutableSet.of())).isFalse();
	}

	@Test
	public void testRetainAll() {
		IAdhocMap aAndB = factory.newMapBuilder(List.of("a", "b")).append("a1").append("b1").build();

		IAdhocMap onlyA = aAndB.retainAll(Set.of("a"));
		Assertions.assertThat((Map) onlyA).isEqualTo(Map.of("a", "a1")).hasSameHashCodeAs(Map.of("a", "a1"));

		IAdhocMap onlyB = aAndB.retainAll(Set.of("b"));
		Assertions.assertThat((Map) onlyB).isEqualTo(Map.of("b", "b1")).hasSameHashCodeAs(Map.of("b", "b1"));
	}

	@Test
	public void testRetainNotIn() {
		IAdhocMap aAndB = factory.newMapBuilder(List.of("a", "b")).append("a1").append("b1").build();

		// disjoint
		IAdhocMap disjoint = aAndB.retainAll(Set.of("c"));
		Assertions.assertThat((Map) disjoint).isEqualTo(Map.of()).hasSameHashCodeAs(Map.of());

		// joint
		IAdhocMap joint = aAndB.retainAll(Set.of("a", "c"));
		Assertions.assertThat((Map) joint).isEqualTo(Map.of("a", "a1")).hasSameHashCodeAs(Map.of("a", "a1"));
	}
}
