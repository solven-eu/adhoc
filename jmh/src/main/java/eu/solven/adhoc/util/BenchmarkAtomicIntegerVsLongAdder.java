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
package eu.solven.adhoc.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Compares increment throughput of {@link AtomicInteger} vs {@link LongAdder} under varying thread counts.
 *
 * <p>
 * {@link AtomicInteger#incrementAndGet()} uses a single CAS loop — fast under low contention but degrades as threads
 * compete on the same cache line. {@link LongAdder#increment()} stripes the counter across multiple cells, reducing
 * contention at the cost of a more expensive {@link LongAdder#sum()} read. The benchmark measures the write-only
 * increment path (no read) to isolate the contention behavior.
 *
 * @author Benoit Lacelle
 */
@SuppressWarnings("checkstyle:MagicNumber")
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkAtomicIntegerVsLongAdder {

	private final AtomicInteger atomicInteger = new AtomicInteger();
	private final LongAdder longAdder = new LongAdder();

	// ---- Single-threaded (no contention) -------------------------------------------------------------------

	@Benchmark
	@Threads(1)
	public int atomicInteger_increment_1thread() {
		return atomicInteger.incrementAndGet();
	}

	@Benchmark
	@Threads(1)
	public void longAdder_increment_1thread() {
		longAdder.increment();
	}

	// ---- Multi-threaded (contention) -----------------------------------------------------------------------

	@Benchmark
	@Threads(4)
	public int atomicInteger_increment_4threads() {
		return atomicInteger.incrementAndGet();
	}

	@Benchmark
	@Threads(4)
	public void longAdder_increment_4threads() {
		longAdder.increment();
	}

	@Benchmark
	@Threads(Threads.MAX)
	public int atomicInteger_increment_maxThreads() {
		return atomicInteger.incrementAndGet();
	}

	@Benchmark
	@Threads(Threads.MAX)
	public void longAdder_increment_maxThreads() {
		longAdder.increment();
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkAtomicIntegerVsLongAdder.class.getSimpleName())
				.forks(1)
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + "-atomicIntegerVsLongAdder.json")
				.build();
		new Runner(opt).run();
	}
}
