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
package eu.solven.adhoc.data.tabular;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
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

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.StandardSliceFactory;

/**
 * Benchmarks related with {@link SliceAsMap#compareTo(SliceAsMap)}
 *
 * @author Benoit Lacelle
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
public class BenchmarkAdhocMapComparateTo {

	StandardSliceFactory factory = StandardSliceFactory.builder().build();

	String c1 = "c1";
	String c2 = "c2";

	IAdhocMap mapA;
	IAdhocMap mapB;
	IAdhocMap mapC;
	IAdhocMap mapD;

	IAdhocSlice sliceMapA;
	IAdhocSlice sliceMapB;
	IAdhocSlice sliceMapC;
	IAdhocSlice sliceMapD;

	@Setup
	public void setup() {
		// some Map
		mapA = factory.newMapBuilder(Set.of("a", "b", "c")).append("a1").append("b1").append(c1).build();
		sliceMapA = SliceAsMap.fromMap(mapA);

		// equals to mapA
		mapB = factory.newMapBuilder(Set.of("a", "b", "c")).append("a1").append("b1").append(c1).build();
		sliceMapB = SliceAsMap.fromMap(mapB);

		// Differ with last value from mapA
		mapC = factory.newMapBuilder(Set.of("a", "b", "c")).append("a1").append("b1").append(c2).build();
		sliceMapC = SliceAsMap.fromMap(mapC);

		// Differ with all/first value from mapA
		mapD = factory.newMapBuilder(Set.of("a", "b", "c")).append("a2").append("b2").append(c2).build();
		sliceMapD = SliceAsMap.fromMap(mapD);
	}

	// Run this method to run benchmarks
	// public static void main(String[] args) throws Exception {
	// org.openjdk.jmh.Main.main(args);
	// }

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkAdhocMapComparateTo.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	@Benchmark
	public int compareEquals_map() {
		return mapA.compareTo(mapB);
	}

	@Benchmark
	public int compareEquals_slice() {
		return sliceMapA.compareTo(sliceMapB);
	}

	// Useful to compare the cost of the string comparison given the map comparison
	@Benchmark
	public int compareLastNotEquals_string() {
		return c1.compareTo(c2);
	}

	@Benchmark
	public int compareLastNotEquals_map() {
		return mapA.compareTo(mapC);
	}

	@Benchmark
	public int compareLastNotEquals_slice() {
		return sliceMapA.compareTo(sliceMapC);
	}

	@Benchmark
	public int compareNoneEquals_map() {
		return mapA.compareTo(mapD);
	}

	@Benchmark
	public int compareNoneEquals_slice() {
		return sliceMapA.compareTo(sliceMapD);
	}

}
