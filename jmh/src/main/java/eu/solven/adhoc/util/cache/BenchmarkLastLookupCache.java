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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Compares three strategies for a last-lookup cache with a single reference key:
 * <ul>
 * <li>{@code CHM} — plain {@link ConcurrentHashMap} lookup (no fast-path).</li>
 * <li>{@code ThreadLocal} — {@link LastLookupCache1}, thread-local last-entry + CHM slow path.</li>
 * <li>{@code Volatile} — {@link LastLookupCache1Volatile}, single shared volatile last-entry + CHM slow path.</li>
 * </ul>
 *
 * The {@link #switchRate} parameter controls how often the benchmark switches to a different key: {@code 0} means the
 * same key every call (best-case fast-path hit rate), higher values (e.g. 4, 16) cycle through more keys so the
 * fast-path rotation matches a realistic query hot loop.
 *
 * @author Benoit Lacelle
 */
@SuppressWarnings({ "checkstyle:MemberName", "checkstyle:MagicNumber", "PMD.AvoidDuplicateLiterals" })
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
public class BenchmarkLastLookupCache {

	/** Number of distinct keys the benchmark rotates through. */
	@Param({ "1", "4", "16" })
	private int keyCount;

	private String[] keys;
	private int keyIndex;

	private ConcurrentMap<Object, String> chmBaseline;
	private LastLookupCache1<String> threadLocalCache;
	private LastLookupCache1Volatile<String> volatileCache;

	@Setup
	public void setup() {
		keys = new String[keyCount];
		for (int i = 0; i < keyCount; i++) {
			// Interned distinct instances; ref-equality will match the same slot across calls.
			keys[i] = ("key-" + i).intern();
		}

		chmBaseline = new ConcurrentHashMap<>();
		threadLocalCache = new LastLookupCache1<>();
		volatileCache = new LastLookupCache1Volatile<>();

		// Pre-populate every backing map so the benchmark measures the fast path, not the first-time compute.
		for (String k : keys) {
			chmBaseline.put(k, "v-" + k);
			threadLocalCache.slowComputeIfAbsent(k, () -> "v-" + k);
			volatileCache.slowComputeIfAbsent(k, () -> "v-" + k);
		}
	}

	private String nextKey() {
		String k = keys[keyIndex];
		int next = keyIndex + 1;
		if (next == keys.length) {
			next = 0;
		}
		keyIndex = next;
		return k;
	}

	// ---- Baseline: plain ConcurrentHashMap ----

	@Benchmark
	public String lookup_chm() {
		return chmBaseline.computeIfAbsent(nextKey(), k -> "v-" + k);
	}

	// ---- ThreadLocal-backed cache ----

	@Benchmark
	public String lookup_threadLocal() {
		String k = nextKey();
		String cached = threadLocalCache.getByRef(k);
		if (cached != null) {
			return cached;
		}
		return threadLocalCache.slowComputeIfAbsent(k, () -> "v-" + k);
	}

	// ---- Volatile-backed cache ----

	@Benchmark
	public String lookup_volatile() {
		String k = nextKey();
		String cached = volatileCache.getByRef(k);
		if (cached != null) {
			return cached;
		}
		return volatileCache.slowComputeIfAbsent(k, () -> "v-" + k);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkLastLookupCache.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + "-lastLookupCache.json")

				.build();
		new Runner(opt).run();
	}
}
