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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.map.MaskedAdhocMap.RetainAllKeys;
import eu.solven.adhoc.map.factory.StandardSliceFactory;

public class TestMaskedAdhocMap {
	StandardSliceFactory factory = StandardSliceFactory.builder().build();

	@Test
	public void testEqualsVerifier() {
		// Does not work due to extending AbstractMap and `keySet` is a private field
		// EqualsVerifier.forClass(MaskedAdhocMap.class).verify();
	}

	@Test
	public void testCompare_withSameMask() {
		IAdhocMap decorated1 = AdhocMapHelpers.fromMap(factory, Map.of("k", "v"));
		MaskedAdhocMap masked1 = MaskedAdhocMap.builder().decorated(decorated1).mask(Map.of("k2", "v2")).build();

		IAdhocMap decorated2 = AdhocMapHelpers.fromMap(factory, Map.of("k", "v"));
		MaskedAdhocMap masked2 = MaskedAdhocMap.builder().decorated(decorated2).mask(Map.of("k2", "v2")).build();

		Assertions.assertThat((Map) masked1).isEqualTo(masked2);
		Assertions.assertThat((Comparable) masked1).isEqualByComparingTo(masked2);
	}

	@Test
	public void testCompare_differentMask() {
		IAdhocMap decorated1 = AdhocMapHelpers.fromMap(factory, Map.of("k", "v"));
		MaskedAdhocMap masked1 = MaskedAdhocMap.builder().decorated(decorated1).mask(Map.of("k2", "v2")).build();

		IAdhocMap decorated2 = AdhocMapHelpers.fromMap(factory, Map.of("k2", "v2"));
		MaskedAdhocMap masked2 = MaskedAdhocMap.builder().decorated(decorated2).mask(Map.of("k", "v")).build();

		Assertions.assertThat((Map) masked1).isEqualTo(masked2);
		Assertions.assertThat((Comparable) masked1).isEqualByComparingTo(masked2);
	}

	@Test
	public void testImmutable() {
		IAdhocMap decorated1 = AdhocMapHelpers.fromMap(factory, Map.of("k", "v"));
		MaskedAdhocMap masked1 = MaskedAdhocMap.builder().decorated(decorated1).mask(Map.of("k2", "v2")).build();

		Assertions.assertThatThrownBy(() -> masked1.clear()).isInstanceOf(UnsupportedOperationException.class);
		// existing entry
		Assertions.assertThatThrownBy(() -> masked1.put("k", "v")).isInstanceOf(UnsupportedOperationException.class);
		// new key
		Assertions.assertThatThrownBy(() -> masked1.put("k3", "v3")).isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	public void testEquals() {
		IAdhocMap decorated = AdhocMapHelpers.fromMap(StandardSliceFactory.builder().build(), Map.of("a", "a1"));
		Map<String, ?> mask = Map.of("b", "b2");
		MaskedAdhocMap masked = MaskedAdhocMap.builder().decorated(decorated).mask(mask).build();

		// `.equals` any Map
		Assertions.assertThat((Map) masked).isEqualTo(Map.of("a", "a1", "b", "b2"));
		Assertions.assertThat(masked.hashCode()).isEqualTo(Map.of("a", "a1", "b", "b2").hashCode());

		// `.get` any key
		Assertions.assertThat(masked.get("a")).isEqualTo("a1");
		Assertions.assertThat(masked.get("b")).isEqualTo("b2");
		Assertions.assertThat(masked.get("c")).isEqualTo(null);
	}

	@Test
	public void testContainsKey() {
		IAdhocMap decorated = Mockito.mock(IAdhocMap.class);
		Map<String, ?> mask = Map.of("b", "b2");
		MaskedAdhocMap masked = MaskedAdhocMap.builder().decorated(decorated).mask(mask).build();

		Assertions.assertThat((Map) masked).doesNotContainKey("a");
		Mockito.verify(decorated, Mockito.times(1)).containsKey("a");

		Assertions.assertThat((Map) masked).containsKey("b");
		Mockito.verify(decorated, Mockito.times(1)).containsKey("b");
	}

	@Test
	public void testRetainAll_excludeMask() {
		IAdhocMap decorated = StandardSliceFactory.builder().build().newMapBuilder(List.of("a")).append("a1").build();
		Map<String, ?> mask = Map.of("b", "b2");
		MaskedAdhocMap masked = MaskedAdhocMap.builder().decorated(decorated).mask(mask).build();

		IAdhocMap retainA = masked.retainAll(Set.of("a"));

		Assertions.assertThat((Map) retainA).isEqualTo(Map.of("a", "a1"));
	}

	@Test
	public void testRetainAll_excludeOnlyMask() {
		IAdhocMap decorated = StandardSliceFactory.builder().build().newMapBuilder(List.of("a")).append("a1").build();
		Map<String, ?> mask = Map.of("b", "b2");
		MaskedAdhocMap masked = MaskedAdhocMap.builder().decorated(decorated).mask(mask).build();

		IAdhocMap retainA = masked.retainAll(Set.of("b"));

		Assertions.assertThat((Map) retainA).isInstanceOfSatisfying(MaskedAdhocMap.class, masked2 -> {
			MaskedAdhocMap masked3 = (MaskedAdhocMap) masked2;
			Assertions.assertThat((Map) masked3.decorated).isEqualTo(Map.of("a", "a1"));
			Assertions.assertThat((Map) masked3.mask).isEqualTo(Map.of("b", "b2"));
		});
	}

	@Test
	public void testRetainAll_excludePartialMaskPartialDecorated() {
		IAdhocMap decorated = StandardSliceFactory.builder()
				.build()
				.newMapBuilder(List.of("a", "b"))
				.append("a1")
				.append("b1")
				.build();
		Map<String, ?> mask = Map.of("c", "c2", "d", "d2");
		MaskedAdhocMap masked = MaskedAdhocMap.builder().decorated(decorated).mask(mask).build();

		IAdhocMap retainA = masked.retainAll(Set.of("b", "c"));

		Assertions.assertThat((Map) retainA).isInstanceOfSatisfying(MaskedAdhocMap.class, masked2 -> {
			MaskedAdhocMap masked3 = (MaskedAdhocMap) masked2;
			Assertions.assertThat((Map) masked3.decorated).isEqualTo(Map.of("b", "b1"));
			Assertions.assertThat((Map) masked3.mask).isEqualTo(Map.of("c", "c2"));
		});
	}

	@Test
	public void testRetainAll_Cache_sameRefMask() {
		Map<String, ?> mask = Map.of("b", "b1");

		RetainAllKeys result = new MaskedAdhocMap.RetainAllKeys(Set.of("a"), mask, Map.of());
		Assertions.assertThat(result.isCompatible(Set.of("a"), mask)).isTrue();
	}

	@Test
	public void testRetainAll_Cache_equalsMask() {
		Map<String, ?> mask = Map.of("b", "b1");

		RetainAllKeys result = new MaskedAdhocMap.RetainAllKeys(Set.of("a"), mask, Map.of());
		Assertions.assertThat(result.isCompatible(Set.of("a"), Map.of("b", "b1"))).isTrue();
	}

	@Test
	public void testRetainAll_Cache_notEqualsSameKeySetMask() {
		Map<String, ?> mask = Map.of("b", "b1");

		RetainAllKeys result = new MaskedAdhocMap.RetainAllKeys(Set.of("a"), mask, Map.of());
		Assertions.assertThat(result.isCompatible(Set.of("a"), Map.of("b", "b2"))).isTrue();
	}

	@Test
	public void testRetainAll_Cache_notEqualsMask() {
		Map<String, ?> mask = Map.of("b", "b1");

		RetainAllKeys result = new MaskedAdhocMap.RetainAllKeys(Set.of("a"), mask, Map.of());
		Assertions.assertThat(result.isCompatible(Set.of("a"), Map.of("c", "b1"))).isFalse();
	}

	@Test
	public void testRetainAll_Cache_retainedLess() {
		Map<String, ?> mask = Map.of("b", "b1");

		RetainAllKeys result = new MaskedAdhocMap.RetainAllKeys(Set.of("a", "b"), mask, Map.of());
		Assertions.assertThat(result.isCompatible(Set.of("a"), mask)).isFalse();
	}

	@Test
	public void testRetainAll_Cache_retainedMore() {
		Map<String, ?> mask = Map.of("b", "b1");

		RetainAllKeys result = new MaskedAdhocMap.RetainAllKeys(Set.of("a", "b"), mask, Map.of());
		Assertions.assertThat(result.isCompatible(Set.of("a", "b", "c"), mask)).isFalse();
	}
}
