/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.encoding.perfect_hashing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueProviderTestHelpers;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.pepper.unittest.MapVerifier;

public class TestMutablePerfectHashMap {

	@Test
	public void testMapContract() {
		IValueProvider p = new IValueProvider() {

			@Override
			public void acceptReceiver(IValueReceiver valueReceiver) {
				// TODO Auto-generated method stub

			}
		};

		IValueProviderTestHelpers.getDouble(p);

		MapVerifier.<String, Integer>forSupplier(MutablePerfectHashMap::new)
				.withSampleKeys("a", "b", "c", "d")
				.withSampleValues(1, 2, 3)
				.preservesInsertionOrder()
				.verify();
	}

	@Test
	public void testEmpty() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		Assertions.assertThat(map).isEmpty();
		Assertions.assertThat(map.size()).isZero();
		Assertions.assertThat(map.get("missing")).isNull();
		Assertions.assertThat(map.containsKey("missing")).isFalse();
		Assertions.assertThat(map.getOrDefault("missing", 42)).isEqualTo(42);
	}

	@Test
	public void testPutThenGet() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		Assertions.assertThat(map.put("a", 1)).isNull();
		Assertions.assertThat(map.put("b", 2)).isNull();
		Assertions.assertThat(map.put("c", 3)).isNull();

		Assertions.assertThat(map.size()).isEqualTo(3);
		Assertions.assertThat(map.get("a")).isEqualTo(1);
		Assertions.assertThat(map.get("b")).isEqualTo(2);
		Assertions.assertThat(map.get("c")).isEqualTo(3);
		Assertions.assertThat(map.get("missing")).isNull();
	}

	@Test
	public void testPutSameKeyReplacesValue() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		map.put("a", 1);
		map.put("b", 2);
		Assertions.assertThat(map.put("a", 10)).isEqualTo(1);

		Assertions.assertThat(map.size()).isEqualTo(2);
		Assertions.assertThat(map.get("a")).isEqualTo(10);
		Assertions.assertThat(map.get("b")).isEqualTo(2);
	}

	@Test
	public void testPutNullValueRejected() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		Assertions.assertThatThrownBy(() -> map.put("a", null)).isInstanceOf(NullPointerException.class);
	}

	@Test
	public void testRemove() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		map.put("a", 1);
		map.put("b", 2);
		map.put("c", 3);

		Assertions.assertThat(map.remove("b")).isEqualTo(2);
		Assertions.assertThat(map.size()).isEqualTo(2);
		Assertions.assertThat(map.containsKey("b")).isFalse();
		Assertions.assertThat(map.get("a")).isEqualTo(1);
		Assertions.assertThat(map.get("c")).isEqualTo(3);

		Assertions.assertThat(map.remove("missing")).isNull();
	}

	@Test
	public void testClear() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		map.put("a", 1);
		map.put("b", 2);

		map.clear();

		Assertions.assertThat(map).isEmpty();
		Assertions.assertThat(map.get("a")).isNull();
		// Usable after clear.
		map.put("c", 3);
		Assertions.assertThat(map.get("c")).isEqualTo(3);
	}

	@Test
	public void testPutAll() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		map.put("a", 1);

		map.putAll(Map.of("b", 2, "c", 3));

		Assertions.assertThat(map).containsExactlyInAnyOrderEntriesOf(Map.of("a", 1, "b", 2, "c", 3));
	}

	@Test
	public void testContainsValue() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		map.put("a", 1);
		map.put("b", 2);

		Assertions.assertThat(map.containsValue(2)).isTrue();
		Assertions.assertThat(map.containsValue(99)).isFalse();
	}

	@Test
	public void testIterationInInsertionOrder() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		map.put("c", 3);
		map.put("a", 1);
		map.put("b", 2);

		Assertions.assertThat(map.keySet()).containsExactly("c", "a", "b");
		Assertions.assertThat(map.values()).containsExactly(3, 1, 2);

		Map<String, Integer> collected = new LinkedHashMap<>();
		map.forEach(collected::put);
		Assertions.assertThat(collected).containsExactly(Map.entry("c", 3), Map.entry("a", 1), Map.entry("b", 2));
	}

	@Test
	public void testEqualsAgainstHashMap() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		map.put("a", 1);
		map.put("b", 2);

		Map<String, Integer> reference = Map.of("a", 1, "b", 2);
		Assertions.assertThat(map).isEqualTo(reference);
		Assertions.assertThat(map.hashCode()).isEqualTo(reference.hashCode());
	}

	@Test
	public void testListKeyType() {
		// Mirrors the ThreadLocalAppendableTable use case: keys are List<String>.
		MutablePerfectHashMap<List<String>, String> map = new MutablePerfectHashMap<>();
		List<String> k1 = List.of("country", "currency");
		List<String> k2 = List.of("country", "year");
		List<String> k3 = List.of("currency");

		map.put(k1, "page1");
		map.put(k2, "page2");
		map.put(k3, "page3");

		Assertions.assertThat(map.get(k1)).isEqualTo("page1");
		Assertions.assertThat(map.get(k2)).isEqualTo("page2");
		Assertions.assertThat(map.get(k3)).isEqualTo("page3");

		// Equal-but-not-same keys still resolve (List.equals is content equality).
		Assertions.assertThat(map.get(List.of("country", "currency"))).isEqualTo("page1");
	}

	@Test
	public void testNonMatchingKeyTypeReturnsNull() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		map.put("a", 1);

		// Wrong-type keys should not crash — Map contract is to return null / false.
		Assertions.assertThat(map.containsKey(42)).isFalse();
	}

	@Test
	public void testGrowthRehashesCorrectly() {
		MutablePerfectHashMap<Integer, Integer> map = new MutablePerfectHashMap<>();
		// Insert enough keys to exercise the rebuild path several times.
		for (int i = 0; i < 32; i++) {
			map.put(i, i * 10);
		}
		Assertions.assertThat(map.size()).isEqualTo(32);
		for (int i = 0; i < 32; i++) {
			Assertions.assertThat(map.get(i)).isEqualTo(i * 10);
		}
	}

	@Test
	public void testRemoveThenGetReflectsRehash() {
		MutablePerfectHashMap<String, Integer> map = new MutablePerfectHashMap<>();
		map.put("a", 1);
		map.put("b", 2);
		map.put("c", 3);

		map.remove("a");

		Assertions.assertThat(map.get("a")).isNull();
		Assertions.assertThat(map.get("b")).isEqualTo(2);
		Assertions.assertThat(map.get("c")).isEqualTo(3);

		// Re-insert a fresh key; indices must be reassigned without corrupting existing slots.
		map.put("d", 4);
		Assertions.assertThat(map.get("b")).isEqualTo(2);
		Assertions.assertThat(map.get("c")).isEqualTo(3);
		Assertions.assertThat(map.get("d")).isEqualTo(4);
	}
}
