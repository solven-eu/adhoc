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

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.map.factory.StandardSliceFactory;

public class TestMaskedAdhocMap {
	StandardSliceFactory factory = StandardSliceFactory.builder().build();

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
}
