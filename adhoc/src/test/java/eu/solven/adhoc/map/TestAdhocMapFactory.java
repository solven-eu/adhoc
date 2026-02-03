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

import java.util.LinkedHashMap;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.map.factory.RowSliceFactory;
import eu.solven.adhoc.map.factory.RowSliceFactory.MapBuilderThroughKeys;

public class TestAdhocMapFactory {
	RowSliceFactory factory = RowSliceFactory.builder().build();

	private void verifyForEach(Map<String, ?> a1b1) {
		Map<String, Object> mapFromForEach = new LinkedHashMap<>();
		a1b1.forEach(mapFromForEach::put);
		Assertions.assertThat(a1b1).isEqualTo(mapFromForEach);

		Map<String, Object> mapFromEntrySet = new LinkedHashMap<>();
		a1b1.entrySet().forEach(e -> mapFromEntrySet.put(e.getKey(), e.getValue()));
		Assertions.assertThat(a1b1).isEqualTo(mapFromEntrySet);
	}

	// @Test
	// public void benchmarkCreateFresh() {
	// // Useful for profiling slow sections, but irrelevant to compare performances
	// for (int i = 0; i < 100000000; i++) {
	// factory.newMapBuilder().put("a", "a1").put("b", "b1").build();
	// }
	// }
	//
	// @Test
	// public void benchmarkCreateFromSet() {
	// // Useful for profiling slow sections, but irrelevant to compare performances
	// for (int i = 0; i < 100000000; i++) {
	// factory.newMapBuilder(ImmutableSet.of("a", "b")).append("a1").append("b1").build();
	// }
	// }

	@Test
	public void testFromFresh() {
		Map<String, ?> a1b1 = factory.newMapBuilder().put("a", "a1").put("b", "b1").build();
		Assertions.assertThat(a1b1).isEqualTo(Map.of("a", "a1", "b", "b1"));
		verifyForEach(a1b1);

		Map<String, ?> b1a1 = factory.newMapBuilder().put("b", "b1").put("a", "a1").build();
		Assertions.assertThat(a1b1).isEqualTo(b1a1);
		verifyForEach(b1a1);

		Map<String, ?> a1b2 = factory.newMapBuilder().put("a", "a1").put("b", "b2").build();
		Assertions.assertThat(a1b1).isNotEqualTo(a1b2);
	}

	@Test
	public void testFromFresh_permutations() {
		Map<String, Object> reference = Map.of("a", "a1", "b", "b1", "c", "c1");
		Collections2.permutations(reference.entrySet().stream().toList()).forEach(permutation -> {
			MapBuilderThroughKeys builder = factory.newMapBuilder();

			permutation.forEach(e -> builder.put(e.getKey(), e.getValue()));

			Assertions.assertThat((Map) builder.build()).isEqualTo(reference);
		});
	}

	@Test
	public void testFromFresh_duplicateKeys() {
		Assertions.assertThatThrownBy(() -> factory.newMapBuilder().put("a", "a1").put("a", "b2").build())
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testFromSet() {
		Map<String, ?> a1b1Set = factory.newMapBuilder(ImmutableSet.of("a", "b")).append("a1").append("b1").build();
		Assertions.assertThat(a1b1Set).isEqualTo(Map.of("a", "a1", "b", "b1"));
		verifyForEach(a1b1Set);

		Map<String, ?> a1b1List = factory.newMapBuilder(ImmutableList.of("a", "b")).append("a1").append("b1").build();
		Assertions.assertThat(a1b1Set).isEqualTo(a1b1List);
		verifyForEach(a1b1List);

		Map<String, ?> b1a1Set = factory.newMapBuilder(ImmutableSet.of("b", "a")).append("b1").append("a1").build();
		Assertions.assertThat(b1a1Set).isEqualTo(a1b1Set);
		Assertions.assertThat(b1a1Set).isEqualTo(a1b1List);
		verifyForEach(b1a1Set);

		Map<String, ?> a1b2List = factory.newMapBuilder(ImmutableList.of("a", "b")).append("a1").append("b2").build();
		Assertions.assertThat(a1b2List).isNotEqualTo(a1b1Set);
		Assertions.assertThat(a1b2List).isNotEqualTo(a1b1List);
		Assertions.assertThat(a1b2List).isNotEqualTo(b1a1Set);
	}

	@Test
	public void testFromSet_duplicateKeys() {
		Assertions
				.assertThatThrownBy(
						() -> factory.newMapBuilder(ImmutableSet.of("a", "a")).append("a1").append("a2").build())
				.isInstanceOf(IllegalArgumentException.class);
	}
}
