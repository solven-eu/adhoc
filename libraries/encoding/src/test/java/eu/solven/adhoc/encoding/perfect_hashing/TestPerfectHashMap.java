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
import java.util.SequencedSet;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.testutil.MapVerifier;

public class TestPerfectHashMap {

	@Test
	public void testMapContract() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b", "c"));
		PerfectHashMap<Object> map =
				PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).put("b", "two").put("c", 3.0).build();

		MapVerifier.forInstance(map).preservesInsertionOrder().verify();
	}

	@Test
	public void testEmptyKeyset() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of());
		PerfectHashMap<Object> map = PerfectHashMap.<Object>newBuilderRandom(keys).build();

		Assertions.assertThat(map).isEmpty();
		Assertions.assertThat(map.size()).isZero();
		Assertions.assertThat(map.isEmpty()).isTrue();
		Assertions.assertThat(map.get("a")).isNull();
		Assertions.assertThat(map.containsKey("a")).isFalse();
	}

	@Test
	public void testFullyPopulated() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b", "c"));
		PerfectHashMap<Object> map =
				PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).put("b", "two").put("c", 3.0).build();

		Assertions.assertThat(map.size()).isEqualTo(3);
		Assertions.assertThat(map.isEmpty()).isFalse();
		Assertions.assertThat(map.get("a")).isEqualTo(1);
		Assertions.assertThat(map.get("b")).isEqualTo("two");
		Assertions.assertThat(map.get("c")).isEqualTo(3.0);
		Assertions.assertThat(map.get("missing")).isNull();
		Assertions.assertThat(map.containsKey("a")).isTrue();
		Assertions.assertThat(map.containsKey("missing")).isFalse();

		Assertions.assertThat(map).containsExactlyInAnyOrderEntriesOf(Map.of("a", 1, "b", "two", "c", 3.0));
	}

	@Test
	public void testPartiallyPopulated() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b", "c"));
		PerfectHashMap<Object> map = PerfectHashMap.<Object>newBuilderRandom(keys).put("b", 42).build();

		Assertions.assertThat(map.size()).isEqualTo(1);
		Assertions.assertThat(map.isEmpty()).isFalse();

		Assertions.assertThat(map.get("a")).isNull();
		Assertions.assertThat(map.containsKey("a")).isFalse();

		Assertions.assertThat(map.get("b")).isEqualTo(42);
		Assertions.assertThat(map.containsKey("b")).isTrue();

		Assertions.assertThat(map.get("c")).isNull();
		Assertions.assertThat(map.containsKey("c")).isFalse();

		Assertions.assertThat(map).containsExactlyInAnyOrderEntriesOf(Map.of("b", 42));
	}

	@Test
	public void testUnknownKey() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b"));
		Assertions.assertThatThrownBy(() -> PerfectHashMap.newBuilderRandom(keys).put("c", 1))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testNullValueRejected() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a"));
		Assertions.assertThatThrownBy(() -> PerfectHashMap.newBuilderRandom(keys).put("a", null))
				.isInstanceOf(NullPointerException.class);
	}

	@Test
	public void testNonStringKeyOnGet() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b"));
		PerfectHashMap<Object> map = PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).build();

		Assertions.assertThat(map.get(42)).isNull();
		Assertions.assertThat(map.containsKey(42)).isFalse();
		Assertions.assertThat(map.get(null)).isNull();
		Assertions.assertThat(map.containsKey(null)).isFalse();
	}

	@Test
	public void testKeysetIsReusable() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b", "c"));

		PerfectHashMap<Object> first = PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).put("b", 2).build();
		PerfectHashMap<Object> second = PerfectHashMap.<Object>newBuilderRandom(keys).put("b", 20).put("c", 30).build();

		Assertions.assertThat(first).containsExactlyInAnyOrderEntriesOf(Map.of("a", 1, "b", 2));
		Assertions.assertThat(second).containsExactlyInAnyOrderEntriesOf(Map.of("b", 20, "c", 30));

		// Keyset (and its perfect hash) is shared by reference.
		Assertions.assertThat(first.getKeyset()).isSameAs(keys);
		Assertions.assertThat(second.getKeyset()).isSameAs(keys);
	}

	@Test
	public void testEqualsAgainstHashMap() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b", "c"));
		PerfectHashMap<Object> map = PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).put("c", 3).build();

		Map<String, Object> reference = Map.of("a", 1, "c", 3);
		Assertions.assertThat(map).isEqualTo(reference);
		Assertions.assertThat(map.hashCode()).isEqualTo(reference.hashCode());
	}

	@Test
	public void testIterationSkipsAbsent() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b", "c", "d"));
		PerfectHashMap<Object> map = PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).put("d", 4).build();

		Assertions.assertThat(map.entrySet()).hasSize(2);
		Assertions.assertThat(map.keySet()).containsExactlyInAnyOrder("a", "d");
		Assertions.assertThat(map.values()).containsExactlyInAnyOrder(1, 4);
	}

	@Test
	public void testKeySetReturnsKeysetWhenFullyPopulated() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b", "c"));
		PerfectHashMap<Object> map =
				PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).put("b", 2).put("c", 3).build();

		// Fast path: keySet() returns the shared PerfectHashKeyset by reference.
		Assertions.assertThat(map.keySet()).isSameAs(keys);
	}

	@Test
	public void testKeySetIsSparseWhenPartial() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b", "c"));
		PerfectHashMap<Object> map = PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).put("c", 3).build();

		Assertions.assertThat(map.keySet()).isNotSameAs(keys);
		Assertions.assertThat(map.keySet()).containsExactly("a", "c");
		Assertions.assertThat(map.keySet().contains("b")).isFalse();
		Assertions.assertThat(map.keySet().contains("a")).isTrue();
	}

	@Test
	public void testForEachSkipsAbsent() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b", "c", "d"));
		PerfectHashMap<Object> map = PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).put("d", 4).build();

		Map<String, Object> collected = new LinkedHashMap<>();
		map.forEach(collected::put);

		Assertions.assertThat(collected).containsExactly(Map.entry("a", 1), Map.entry("d", 4));
	}

	@Test
	public void testGetOrDefault() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("a", "b"));
		PerfectHashMap<Object> map = PerfectHashMap.<Object>newBuilderRandom(keys).put("a", 1).build();

		Assertions.assertThat(map.getOrDefault("a", "fallback")).isEqualTo(1);
		Assertions.assertThat(map.getOrDefault("b", "fallback")).isEqualTo("fallback");
		Assertions.assertThat(map.getOrDefault("missing", "fallback")).isEqualTo("fallback");
	}

	// PerfectHashKeyset is itself a SequencedSet — exercised here to lock the contract in.

	@Test
	public void testKeysetAsSequencedSet() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("alpha", "beta", "gamma"));

		Assertions.assertThat(keys).containsExactly("alpha", "beta", "gamma");
		Assertions.assertThat(keys.size()).isEqualTo(3);
		Assertions.assertThat(keys.contains("beta")).isTrue();
		Assertions.assertThat(keys.contains("missing")).isFalse();
		Assertions.assertThat(keys.contains(42)).isFalse();
		Assertions.assertThat(keys.getFirst()).isEqualTo("alpha");
		Assertions.assertThat(keys.getLast()).isEqualTo("gamma");
		Assertions.assertThat(keys.indexOf("beta")).isEqualTo(1);
		Assertions.assertThat(keys.indexOf("missing")).isEqualTo(-1);
	}

	@Test
	public void testKeysetReversed() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of("alpha", "beta", "gamma"));

		SequencedSet<String> reversed = keys.reversed();
		Assertions.assertThat(reversed).containsExactly("gamma", "beta", "alpha");
		Assertions.assertThat(reversed.getFirst()).isEqualTo("gamma");
		Assertions.assertThat(reversed.getLast()).isEqualTo("alpha");
	}

	@Test
	public void testKeysetEmpty() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of());

		Assertions.assertThat(keys).isEmpty();
		Assertions.assertThat(keys.size()).isZero();
		Assertions.assertThat(keys.contains("a")).isFalse();
	}
}
