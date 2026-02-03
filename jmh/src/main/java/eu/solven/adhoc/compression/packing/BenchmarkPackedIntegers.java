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
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import eu.solven.adhoc.compression.IIntArray;

/**
 * Benchmarks related with {@link FlexiblePackedIntegers}
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

	final int[] array7 = IntStream.range(0, 7).toArray();
	final IIntArray packed7 = FlexiblePackedIntegers.doPack(array7);

	final int[] array1024 = IntStream.range(0, 1024).toArray();
	final IIntArray packed1024 = FlexiblePackedIntegers.doPack(array1024);

	final int[] array255 = IntStream.range(0, 255).toArray();
	final IIntArray packed255SingleChunk = FlexiblePackedIntegers.doPack(array255);
	final IIntArray packed255Flexible = FlexiblePackedIntegers.asFlexible(packed255SingleChunk);

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkPackedIntegers.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	@Setup
	public void setup() {
		if (!(packed255Flexible instanceof FlexiblePackedIntegers)) {
			throw new IllegalStateException("Should be %s".formatted(FlexiblePackedIntegers.class.getName()));
		}
		if (!(packed255SingleChunk instanceof SingleChunkPackedIntegers)) {
			throw new IllegalStateException("Should be %s".formatted(SingleChunkPackedIntegers.class.getName()));
		}
	}

	@Benchmark
	public int readInt_given7_read0() {
		return packed7.readInt(0);
	}

	@Benchmark
	public int readInt_given255_read255_flexible() {
		return packed255Flexible.readInt(254);
	}

	// On an equivalent case, singleChunk should be (more or less) faster than flexible.
	@Benchmark
	public int readInt_given255_read255_singlechunk() {
		return packed255SingleChunk.readInt(254);
	}

	@Benchmark
	public int readInt_given1024_read0() {
		return packed1024.readInt(0);
	}

	@Benchmark
	@OperationsPerInvocation(7)
	public IIntArray pack_0to7() {
		return FlexiblePackedIntegers.doPack(array7);
	}

	@Benchmark
	@OperationsPerInvocation(255)
	public IIntArray pack_0to255() {
		return FlexiblePackedIntegers.doPack(array255);
	}

	@Benchmark
	@OperationsPerInvocation(1024)
	public IIntArray pack_0to1024() {
		return FlexiblePackedIntegers.doPack(array1024);
	}

}
