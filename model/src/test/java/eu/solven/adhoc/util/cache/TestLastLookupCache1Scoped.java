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
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLastLookupCache1Scoped {

	@Test
	public void testGetByRef_outOfScope_returnsNull() {
		LastLookupCache1Scoped<String> cache = new LastLookupCache1Scoped<>();
		Assertions.assertThat(cache.getByRef("k")).isNull();
	}

	@Test
	public void testSlowComputeIfAbsent_outOfScope_stillComputes() {
		LastLookupCache1Scoped<String> cache = new LastLookupCache1Scoped<>();

		AtomicInteger invocations = new AtomicInteger();
		String value = cache.slowComputeIfAbsent("k", () -> {
			invocations.incrementAndGet();
			return "computed";
		});

		Assertions.assertThat(value).isEqualTo("computed");
		Assertions.assertThat(invocations.get()).isEqualTo(1);

		// Without a scope, the holder is not updated — so getByRef still misses.
		Assertions.assertThat(cache.getByRef("k")).isNull();
	}

	@Test
	public void testFastPathHit_insideScope_afterSlowPath() {
		LastLookupCache1Scoped<String> cache = new LastLookupCache1Scoped<>();
		Object k = new Object();

		AtomicReference<String> observed = new AtomicReference<>();
		cache.runInScope(() -> {
			cache.slowComputeIfAbsent(k, () -> "computed");
			observed.set(cache.getByRef(k));
		});

		Assertions.assertThat(observed.get()).isEqualTo("computed");
	}

	@Test
	public void testFastPathMiss_differentReference_insideScope() {
		LastLookupCache1Scoped<String> cache = new LastLookupCache1Scoped<>();
		String k1 = new String("key"); // NOSONAR — intentional new instance for ref-equality test
		String k2 = new String("key");

		AtomicReference<String> observed = new AtomicReference<>("unset");
		cache.runInScope(() -> {
			cache.slowComputeIfAbsent(k1, () -> "computed");
			observed.set(cache.getByRef(k2));
		});

		Assertions.assertThat(observed.get()).isNull();
	}

	@Test
	public void testSlowPath_hitsBackingMapForEqualKey() {
		LastLookupCache1Scoped<String> cache = new LastLookupCache1Scoped<>();
		String k1 = new String("key"); // NOSONAR
		String k2 = new String("key");

		AtomicInteger invocations = new AtomicInteger();
		cache.runInScope(() -> {
			cache.slowComputeIfAbsent(k1, () -> {
				invocations.incrementAndGet();
				return "computed";
			});
		});

		AtomicReference<String> second = new AtomicReference<>();
		cache.runInScope(() -> {
			// Fresh scope: the holder is empty, so getByRef misses even for an equals key.
			Assertions.assertThat(cache.getByRef(k2)).isNull();

			second.set(cache.slowComputeIfAbsent(k2, () -> {
				invocations.incrementAndGet();
				return "recomputed";
			}));
		});

		Assertions.assertThat(second.get()).isEqualTo("computed");
		Assertions.assertThat(invocations.get()).isEqualTo(1);
	}

	@Test
	public void testScopeIsolation_freshScopeStartsCold() {
		LastLookupCache1Scoped<String> cache = new LastLookupCache1Scoped<>();
		Object k = new Object();

		cache.runInScope(() -> cache.slowComputeIfAbsent(k, () -> "v"));

		// New scope: holder is a fresh Holder, so the fast path misses the first time.
		AtomicReference<String> fast = new AtomicReference<>("unset");
		cache.runInScope(() -> fast.set(cache.getByRef(k)));

		Assertions.assertThat(fast.get()).isNull();
	}

	@Test
	public void testCallInScope_propagatesResult() throws Exception {
		LastLookupCache1Scoped<String> cache = new LastLookupCache1Scoped<>();
		Object k = new Object();

		String result = cache.callInScope(() -> {
			cache.slowComputeIfAbsent(k, () -> "computed");
			return cache.getByRef(k);
		});

		Assertions.assertThat(result).isEqualTo("computed");
	}

	@Test
	public void testClear_emptiesBackingMap() {
		LastLookupCache1Scoped<String> cache = new LastLookupCache1Scoped<>();
		AtomicInteger invocations = new AtomicInteger();

		Object k = new Object();
		cache.runInScope(() -> cache.slowComputeIfAbsent(k, () -> {
			invocations.incrementAndGet();
			return "v";
		}));

		cache.clear();

		// Fresh scope after clear: backing map is empty, so the supplier runs again.
		cache.runInScope(() -> cache.slowComputeIfAbsent(k, () -> {
			invocations.incrementAndGet();
			return "v2";
		}));

		Assertions.assertThat(invocations.get()).isEqualTo(2);
	}
}
