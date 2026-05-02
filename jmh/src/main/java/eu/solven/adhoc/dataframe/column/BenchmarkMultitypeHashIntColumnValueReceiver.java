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
package eu.solven.adhoc.dataframe.column;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

import eu.solven.adhoc.dataframe.column.hash.MultitypeHashIntColumn;
import eu.solven.adhoc.primitive.IValueReceiver;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

/**
 * Measures the cost of the {@link IValueReceiver} / {@code IValueProvider} indirection layer on the hot path of
 * {@link MultitypeHashIntColumn}. The column's public API does not expose primitive {@code long} reads/writes: every
 * write goes through an {@code IValueReceiver} returned by {@link MultitypeHashIntColumn#append(int)}, and every read
 * goes through an {@code IValueProvider} returned by {@link MultitypeHashIntColumn#onValue(int)} that pushes its value
 * into a caller-supplied {@code IValueReceiver}. Both indirections allocate a fresh instance per call — an anonymous
 * inner class on {@code append} (captures the {@code int key} final local) and a lambda on {@code onValue}. Whether
 * those allocations are scalar-replaced by escape analysis is exactly the open question this benchmark answers.
 *
 * <p>
 * For each of write and read, two variants are measured over an identical workload (same {@code int[]} keys and
 * {@code long[]} values):
 * <ul>
 * <li><b>raw</b> — direct fastutil {@link Int2LongOpenHashMap#put(int, long)} / {@link Int2LongOpenHashMap#get(int)}.
 * This is the zero-abstraction baseline. A sibling {@code Int2LongOpenHashMap} is used rather than reaching into
 * {@link MultitypeHashIntColumn}'s package-private {@code sliceToL} field so the benchmark stays in its own module; the
 * delta therefore also captures any non-sliceToL bookkeeping the column performs on the hot path (e.g.
 * {@code checkSizeBeforeAdd}, the 3-lane {@code containsKey} probe in {@code append}).</li>
 * <li><b>wrapped</b> — goes through the public column API. The write path allocates one {@code IValueReceiver} per
 * {@code append(int)} call; the read path allocates one {@code IValueProvider} per {@code onValue(int)} call and feeds
 * it into a pre-allocated {@code IValueReceiver} sink that writes into an {@link AtomicInteger} (held at {@code @State}
 * scope so its creation is not counted).</li>
 * </ul>
 *
 * <p>
 * The sink receiver is <strong>pre-allocated once</strong> in {@link #setupKeys()} and reused across every benchmark
 * invocation, matching the real call site the user is worried about: a combination or scanner that reads many slices
 * through the same long-lived receiver. The {@link AtomicInteger} creation is not measured per the benchmark spec.
 *
 * <p>
 * Read the headline delta as "cost of the {@code IValueReceiver}/{@code IValueProvider} indirection on the hot path" —
 * a headline figure close to zero means escape analysis scalar-replaces the intermediate allocations and the design is
 * free; a meaningful gap means the JIT cannot elide the chain and the column pays for the abstraction.
 *
 * @author Benoit Lacelle
 */
@SuppressWarnings({ "checkstyle:MemberName", "checkstyle:MagicNumber", "PMD.AvoidDuplicateLiterals" })
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
public class BenchmarkMultitypeHashIntColumnValueReceiver {

	/** Number of distinct int keys processed per benchmark invocation. */
	@Param({ "1024", "16384" })
	private int size;

	/** Pre-built key/value arrays — same instances reused across iterations. */
	private int[] keys;
	private long[] values;

	/**
	 * Reusable sink: the {@link AtomicInteger} whose {@code set} is invoked by the read-path sink receiver. Held at
	 * {@code @State} scope per the benchmark contract — its creation is not part of any measurement.
	 */
	private AtomicInteger sink;

	/**
	 * Pre-allocated {@link IValueReceiver} that writes into {@link #sink}. Reused across every
	 * {@code onValue(...).acceptReceiver(sinkReceiver)} call in the wrapped read benchmark.
	 */
	private IValueReceiver sinkReceiver;

	/**
	 * Pre-populated {@link Int2LongOpenHashMap} used by the raw read benchmark. Populated once at trial setup.
	 */
	private Int2LongOpenHashMap rawMapRead;

	/**
	 * Pre-populated {@link MultitypeHashIntColumn} used by the wrapped read benchmark. Populated once at trial setup
	 * via the wrapped write path.
	 */
	private MultitypeHashIntColumn columnRead;

	/**
	 * Fresh {@link Int2LongOpenHashMap} rebuilt per invocation for the raw write benchmark, so each invocation writes
	 * into an empty map.
	 */
	private Int2LongOpenHashMap rawMapWrite;

	/**
	 * Fresh {@link MultitypeHashIntColumn} rebuilt per invocation for the wrapped write benchmark, so each invocation
	 * writes into an empty column.
	 */
	private MultitypeHashIntColumn columnWrite;

	@Setup(Level.Trial)
	public void setupKeys() {
		keys = new int[size];
		values = new long[size];
		for (int i = 0; i < size; i++) {
			keys[i] = i;
			// Non-zero, non-sequential values so the JIT cannot fold the loop into a constant.
			values[i] = (long) i * 31L + 17L;
		}

		sink = new AtomicInteger();
		sinkReceiver = new IValueReceiver() {
			@Override
			public void onLong(long v) {
				sink.set((int) v);
			}

			@Override
			@SuppressWarnings("checkstyle:AvoidInlineConditionals")
			public void onObject(Object v) {
				// The wrapped read benchmark only ever stores long values; this branch exists only to satisfy
				// the IValueReceiver contract.
				sink.set(v == null ? 0 : 1);
			}
		};

		// Pre-populate the read targets once per trial.
		rawMapRead = new Int2LongOpenHashMap(size);
		columnRead = MultitypeHashIntColumn.builder().capacity(size).build();
		for (int i = 0; i < size; i++) {
			rawMapRead.put(keys[i], values[i]);
			columnRead.append(keys[i]).onLong(values[i]);
		}
	}

	@Setup(Level.Invocation)
	public void resetWriteTargets() {
		rawMapWrite = new Int2LongOpenHashMap(size);
		columnWrite = MultitypeHashIntColumn.builder().capacity(size).build();
	}

	// ---- WRITE benchmarks ----------------------------------------------------------------------------------

	/**
	 * Baseline write: direct fastutil put. No {@link IValueReceiver} allocation on the hot path.
	 */
	@Benchmark
	public Int2LongOpenHashMap write_raw_int2LongPut() {
		Int2LongOpenHashMap target = rawMapWrite;
		int[] ks = keys;
		long[] vs = values;
		for (int i = 0, n = size; i < n; i++) {
			target.put(ks[i], vs[i]);
		}
		return target;
	}

	/**
	 * Wrapped write: goes through {@link MultitypeHashIntColumn#append(int)} which returns a fresh anonymous
	 * {@code IValueReceiver} capturing the {@code int key} final local. If escape analysis works, the receiver is
	 * scalar-replaced and this should be close to the raw baseline.
	 */
	@Benchmark
	public MultitypeHashIntColumn write_wrapped_appendOnLong() {
		MultitypeHashIntColumn target = columnWrite;
		int[] ks = keys;
		long[] vs = values;
		for (int i = 0, n = size; i < n; i++) {
			target.append(ks[i]).onLong(vs[i]);
		}
		return target;
	}

	// ---- READ benchmarks -----------------------------------------------------------------------------------

	/**
	 * Baseline read: direct fastutil get into the pre-allocated sink. No {@code IValueProvider} allocation on the hot
	 * path.
	 */
	@Benchmark
	public int read_raw_int2LongGet() {
		Int2LongOpenHashMap target = rawMapRead;
		AtomicInteger s = sink;
		int[] ks = keys;
		for (int i = 0, n = size; i < n; i++) {
			s.set((int) target.get(ks[i]));
		}
		return s.get();
	}

	/**
	 * Wrapped read: each call to {@link MultitypeHashIntColumn#onValue(int)} returns a fresh {@code IValueProvider}
	 * lambda that captures {@code this} and the {@code int key} final local; the lambda pushes its value into the
	 * pre-allocated {@link #sinkReceiver}. The sink receiver is allocated once at trial setup — this benchmark measures
	 * the per-call {@code IValueProvider} allocation plus the cost of the virtual dispatch through it.
	 */
	@Benchmark
	public int read_wrapped_onValueAcceptReceiver() {
		MultitypeHashIntColumn target = columnRead;
		IValueReceiver receiver = sinkReceiver;
		int[] ks = keys;
		for (int i = 0, n = size; i < n; i++) {
			target.onValue(ks[i]).acceptReceiver(receiver);
		}
		return sink.get();
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkMultitypeHashIntColumnValueReceiver.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + "-multitypeHashIntColumnValueReceiver.json")

				.build();
		new Runner(opt).run();
	}
}
