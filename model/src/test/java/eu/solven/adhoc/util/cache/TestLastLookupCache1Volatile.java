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

import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLastLookupCache1Volatile {

	@Test
	public void testInitialLookup_missesAndReturnsNull() {
		LastLookupCache1Volatile<String> cache = new LastLookupCache1Volatile<>();
		Assertions.assertThat(cache.getByRef("k")).isNull();
	}

	@Test
	public void testSlowComputeIfAbsent_populatesAndReturnsValue() {
		LastLookupCache1Volatile<String> cache = new LastLookupCache1Volatile<>();

		AtomicInteger invocations = new AtomicInteger();
		String value = cache.slowComputeIfAbsent("k", () -> {
			invocations.incrementAndGet();
			return "computed";
		});

		Assertions.assertThat(value).isEqualTo("computed");
		Assertions.assertThat(invocations.get()).isEqualTo(1);
	}

	@Test
	public void testFastPathHit_afterSlowPath() {
		LastLookupCache1Volatile<String> cache = new LastLookupCache1Volatile<>();
		Object k = new Object();

		cache.slowComputeIfAbsent(k, () -> "computed");

		Assertions.assertThat(cache.getByRef(k)).isEqualTo("computed");
	}

	@Test
	public void testFastPathMiss_differentReference() {
		LastLookupCache1Volatile<String> cache = new LastLookupCache1Volatile<>();
		String k1 = new String("key"); // NOSONAR — intentional new instance for ref-equality test
		String k2 = new String("key");

		cache.slowComputeIfAbsent(k1, () -> "computed");

		Assertions.assertThat(cache.getByRef(k2)).isNull();
	}

	@Test
	public void testSlowPath_hitsBackingMapForEqualKey() {
		LastLookupCache1Volatile<String> cache = new LastLookupCache1Volatile<>();
		String k1 = new String("key"); // NOSONAR
		String k2 = new String("key");

		AtomicInteger invocations = new AtomicInteger();
		cache.slowComputeIfAbsent(k1, () -> {
			invocations.incrementAndGet();
			return "computed";
		});

		String second = cache.slowComputeIfAbsent(k2, () -> {
			invocations.incrementAndGet();
			return "recomputed";
		});

		Assertions.assertThat(second).isEqualTo("computed");
		Assertions.assertThat(invocations.get()).isEqualTo(1);
	}

	@Test
	public void testClear_emptiesBackingMapAndResetsSlot() {
		LastLookupCache1Volatile<String> cache = new LastLookupCache1Volatile<>();
		AtomicInteger invocations = new AtomicInteger();

		Object k = new Object();
		cache.slowComputeIfAbsent(k, () -> {
			invocations.incrementAndGet();
			return "v";
		});

		cache.clear();

		// Fast path miss after clear.
		Assertions.assertThat(cache.getByRef(k)).isNull();

		cache.slowComputeIfAbsent(k, () -> {
			invocations.incrementAndGet();
			return "v2";
		});

		Assertions.assertThat(invocations.get()).isEqualTo(2);
	}
}
