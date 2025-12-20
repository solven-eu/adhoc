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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.dictionary.DictionarizedSliceFactory;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.ASliceFactory.IHasEntries;

public class TestDictionarizedSliceFactory {
	DictionarizedSliceFactory sliceFactory = DictionarizedSliceFactory.builder().build();

	@Test
	public void testEmpty() {
		IAdhocMap map = sliceFactory.newMapBuilder().build();
		Assertions.assertThat((Map) map).isEqualTo(Map.of()).isEmpty();
		Assertions.assertThat(map.entrySet()).isEqualTo(Set.of()).isEmpty();
	}

	@Test
	public void testOneEntry() {
		IAdhocMap map = sliceFactory.newMapBuilder().put("c", "v").build();
		Assertions.assertThat((Map) map).isEqualTo(Map.of("c", "v"));

		Assertions.assertThat(((AbstractAdhocMap) map).getSortedValueRaw(0)).isEqualTo("v");
	}

	@Test
	public void testTwoEntry_reversed() {
		IAdhocMap map = sliceFactory.newMapBuilder().put("b", "vB").put("a", "vA").build();
		Assertions.assertThat((Map) map).isEqualTo(Map.of("a", "vA", "b", "vB"));

		Assertions.assertThat(((AbstractAdhocMap) map).getSortedValueRaw(0)).isEqualTo("vB");
		Assertions.assertThat(((AbstractAdhocMap) map).getSortedValueRaw(1)).isEqualTo("vA");

		Assertions.assertThat(((AbstractAdhocMap) map).getSequencedValueRaw(0)).isEqualTo("vB");
		Assertions.assertThat(((AbstractAdhocMap) map).getSequencedValueRaw(1)).isEqualTo("vA");
	}

	@Test
	public void testPre_Empty() {
		IAdhocMap map = sliceFactory.newMapBuilder(Set.of()).build();
		Assertions.assertThat((Map) map).isEqualTo(Map.of());
	}

	@Test
	public void testPre_OneEntry() {
		IAdhocMap map = sliceFactory.newMapBuilder(ImmutableSet.of("c")).append("v").build();
		Assertions.assertThat((Map) map).isEqualTo(Map.of("c", "v"));
	}

	@Test
	public void testBuildRawMaps() {
		IAdhocMap map = sliceFactory.buildMap(new IHasEntries() {

			@Override
			public Collection<? extends String> getKeys() {
				return List.of("a", "b");
			}

			@Override
			public Collection<?> getValues() {
				return List.of("vA", "vB");
			}

		});
		Assertions.assertThat((Map) map).isEqualTo(Map.of("a", "vA", "b", "vB"));
	}

	@Test
	public void testCompare() {
		IAdhocMap c1v1 = sliceFactory.newMapBuilder(ImmutableSet.of("c1")).append("v1").build();
		IAdhocMap c1v2 = sliceFactory.newMapBuilder(ImmutableSet.of("c1")).append("v2").build();
		IAdhocMap c2v1 = sliceFactory.newMapBuilder(ImmutableSet.of("c2")).append("v1").build();
		IAdhocMap c2v2 = sliceFactory.newMapBuilder(ImmutableSet.of("c2")).append("v2").build();

		Assertions.assertThat((Comparable) c1v1).isLessThan(c1v2).isLessThan(c2v1).isLessThan(c2v2);
		Assertions.assertThat((Comparable) c1v2).isGreaterThan(c1v1).isLessThan(c2v1).isLessThan(c2v2);
		Assertions.assertThat((Comparable) c2v1).isGreaterThan(c1v1).isGreaterThan(c1v2).isLessThan(c2v2);
		Assertions.assertThat((Comparable) c2v2).isGreaterThan(c1v1).isGreaterThan(c1v2).isGreaterThan(c2v1);
	}
}
