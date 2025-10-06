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
package eu.solven.adhoc.engine.step;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * Benchmarks related with {@link SliceAsMapWithStep}
 * 
 * @author Benoit Lacelle
 */
@SuppressWarnings("checkstyle:MemberName")
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
public class BenchmarkSliceAsMapWithStep {

	final ISliceFilter filterAB = AndFilter.and(Map.of("a", "a1", "b", "b1"));
	final ISliceFilter filterBC = AndFilter.and(Map.of("b", "b1", "c", "c1"));

	final ISliceFilter filterA_optimized = FilterBuilder.and(filterAB, filterBC).combine();
	final ISliceFilter filterA_notOptimized = AndFilter.builder().and(filterAB).and(filterBC).build();

	final ISliceReader filterA_SliceFilter = SliceReader.builder().sliceFilter(filterAB).stepFilter(filterBC).build();

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkSliceAsMapWithStep.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	@Benchmark
	public ISliceFilter and_optimized() {
		return FilterBuilder.and(filterAB, filterBC).optimize();
	}

	@Benchmark
	public ISliceFilter and_notOptimized() {
		return AndFilter.builder().and(filterAB).and(filterBC).build();
	}

	@Benchmark
	public IValueMatcher getColumn_andOptimized() {
		return FilterHelpers.getValueMatcher(filterA_optimized, "a");
	}

	@Benchmark
	public IValueMatcher getColumn_andNotOptimized() {
		return FilterHelpers.getValueMatcher(filterA_notOptimized, "a");
	}

	@Benchmark
	public IValueMatcher getColumn_sliceReader() {
		return filterA_SliceFilter.getValueMatcher("a");
	}

}
