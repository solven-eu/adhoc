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
package eu.solven.adhoc.util.cache;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLastLookupCache {

	@Test
	public void testConstructor_rejectsInvalidKeyLength() {
		Assertions.assertThatThrownBy(() -> new LastLookupCache<String>(0))
				.isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThatThrownBy(() -> new LastLookupCache<String>(-1))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testKeyLength() {
		LastLookupCache<String> cache = new LastLookupCache<>(3);
		Assertions.assertThat(cache.getKeyLength()).isEqualTo(3);
	}

	@Test
	public void testInitialLookup_missesAndReturnsNull() {
		LastLookupCache<String> cache = new LastLookupCache<>(2);
		Assertions.assertThat(cache.getByRef("a", "b")).isNull();
	}

	@Test
	public void testSlowComputeIfAbsent_populatesAndReturnsValue() {
		LastLookupCache<String> cache = new LastLookupCache<>(2);

		AtomicInteger invocations = new AtomicInteger();
		String value = cache.slowComputeIfAbsent(new Object[] { "a", "b" }, () -> {
			invocations.incrementAndGet();
			return "computed";
		});

		Assertions.assertThat(value).isEqualTo("computed");
		Assertions.assertThat(invocations.get()).isEqualTo(1);
	}

	@Test
	public void testFastPathHit_afterSlowPath() {
		LastLookupCache<String> cache = new LastLookupCache<>(2);
		Object k1 = new Object();
		Object k2 = new Object();

		cache.slowComputeIfAbsent(new Object[] { k1, k2 }, () -> "computed");

		// Subsequent fast-path lookup with the SAME references hits.
		Assertions.assertThat(cache.getByRef(k1, k2)).isEqualTo("computed");
	}

	@Test
	public void testFastPathMiss_differentReference() {
		LastLookupCache<String> cache = new LastLookupCache<>(2);
		String k1 = new String("key"); // NOSONAR — intentional new instance for ref-equality test
		String k2 = new String("key");

		cache.slowComputeIfAbsent(new Object[] { "anchor", k1 }, () -> "computed");

		// Same value, different reference → miss
		Assertions.assertThat(cache.getByRef("anchor", k2)).isNull();
	}

	@Test
	public void testSlowPath_hitsBackingMapForEqualKey() {
		LastLookupCache<String> cache = new LastLookupCache<>(2);
		String k1 = new String("key"); // NOSONAR
		String k2 = new String("key");

		AtomicInteger invocations = new AtomicInteger();
		cache.slowComputeIfAbsent(new Object[] { "anchor", k1 }, () -> {
			invocations.incrementAndGet();
			return "computed";
		});

		// Different reference → fast-path miss → slow-path hits the backing map by equals/hashCode.
		String second = cache.slowComputeIfAbsent(new Object[] { "anchor", k2 }, () -> {
			invocations.incrementAndGet();
			return "recomputed";
		});

		Assertions.assertThat(second).isEqualTo("computed");
		Assertions.assertThat(invocations.get()).isEqualTo(1);
	}

	@Test
	public void testClear_emptiesBackingMap() {
		LastLookupCache<String> cache = new LastLookupCache<>(1);
		AtomicInteger invocations = new AtomicInteger();

		cache.slowComputeIfAbsent(new Object[] { "k" }, () -> {
			invocations.incrementAndGet();
			return "v";
		});

		cache.clear();

		// After clear, slow-path recomputes.
		cache.slowComputeIfAbsent(new Object[] { new String("k") /* NOSONAR */ }, () -> {
			invocations.incrementAndGet();
			return "v2";
		});

		Assertions.assertThat(invocations.get()).isEqualTo(2);
	}

	@Test
	public void testKeyLength1_singleKeyPart() {
		LastLookupCache<String> cache = new LastLookupCache<>(1);
		Object k = new Object();

		cache.slowComputeIfAbsent(new Object[] { k }, () -> "v");
		Assertions.assertThat(cache.getByRef(k)).isEqualTo("v");
	}

	@Test
	public void testKeyLength3_multipleKeyParts() {
		LastLookupCache<List<String>> cache = new LastLookupCache<>(3);
		Object k1 = new Object();
		Object k2 = new Object();
		Object k3 = new Object();

		cache.slowComputeIfAbsent(new Object[] { k1, k2, k3 }, () -> List.of("a", "b"));
		Assertions.assertThat(cache.getByRef(k1, k2, k3)).containsExactly("a", "b");

		// Changing the last reference misses on the fast path.
		Assertions.assertThat(cache.getByRef(k1, k2, new Object())).isNull();
	}
}
