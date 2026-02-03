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
package eu.solven.adhoc.compression.packing;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks related with {@link PackedIntegers}
 *
 * @author Benoit Lacelle
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.Throughput)
@Fork(value = 1)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@SuppressWarnings("checkstyle:MagicNumber")
public class BenchmarkPackedIntegers {

	private int[] array7 = IntStream.range(0, 7).toArray();
	PackedIntegers packed7 = PackedIntegers.doPack(array7);

	private int[] array1024 = IntStream.range(0, 1024).toArray();
	PackedIntegers packed1024 = PackedIntegers.doPack(array1024);

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkPackedIntegers.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	@Benchmark
	public int readInt_given7_read0() {
		return packed7.readInt(0);
	}

	@Benchmark
	public int readInt_given1024_read0() {
		return packed1024.readInt(0);
	}

	@Benchmark
	@OperationsPerInvocation(7)
	public PackedIntegers pack_0to7() {
		return PackedIntegers.doPack(array7);
	}

	@Benchmark
	@OperationsPerInvocation(1024)
	public PackedIntegers pack_0to1024() {
		return PackedIntegers.doPack(array1024);
	}

}
