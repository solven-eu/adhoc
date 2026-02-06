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
package eu.solven.adhoc.compression;

import java.util.List;
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

import eu.solven.adhoc.compression.packing.FlexiblePackedIntegers;

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
public class BenchmarkMapDictionarizer {

	List<String> strings = IntStream.range(0, 1024).mapToObj(Integer::toString).toList();

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkMapDictionarizer.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	@OperationsPerInvocation(16)
	@Benchmark
	public IDictionarizer map16() {
		MapDictionarizer d = MapDictionarizer.builder().build();

		for (int i = 0; i < 16; i++) {
			d.toInt(strings.get(i));
		}

		return d;
	}

	@OperationsPerInvocation(128)
	@Benchmark
	public IDictionarizer map128() {
		MapDictionarizer d = MapDictionarizer.builder().build();

		for (int i = 0; i < 128; i++) {
			d.toInt(strings.get(i));
		}

		return d;
	}

	@OperationsPerInvocation(1024)
	@Benchmark
	public IDictionarizer map1024() {
		MapDictionarizer d = MapDictionarizer.builder().build();

		for (int i = 0; i < 1024; i++) {
			d.toInt(strings.get(i));
		}

		return d;
	}


	@OperationsPerInvocation(16)
	@Benchmark
	public IDictionarizer map16_agrona() {
		MapDictionarizer d = MapDictionarizer.builder().build();

		for (int i = 0; i < 16; i++) {
			d.toInt(strings.get(i));
		}

		return d;
	}

	@OperationsPerInvocation(128)
	@Benchmark
	public IDictionarizer map128_agrona() {
		MapDictionarizer d = MapDictionarizer.builder().build();

		for (int i = 0; i < 128; i++) {
			d.toInt(strings.get(i));
		}

		return d;
	}

	@OperationsPerInvocation(1024)
	@Benchmark
	public IDictionarizer map1024_agrona() {
		MapDictionarizer d = MapDictionarizer.builder().build();

		for (int i = 0; i < 1024; i++) {
			d.toInt(strings.get(i));
		}

		return d;
	}

}
