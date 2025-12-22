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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.MapComparators;
import eu.solven.adhoc.map.factory.StandardSliceFactory;

/**
 * Benchmarks comparing {@link IAdhocMap} versus {@link HashMap}.
 *
 * @author Benoit Lacelle
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
public class BenchmarkAdhocMapVsHashMap {

	StandardSliceFactory factory = StandardSliceFactory.builder().build();

	IAdhocMap adhocMap;
	Map<String, Object> hashMap;

	IAdhocMap adhocMap2;
	Map<String, Object> hashMap2;

	AtomicInteger length = new AtomicInteger();

	@Setup
	public void setup() {
		adhocMap = factory.newMapBuilder(Set.of("a", "b", "c")).append("a1").append("b1").append("c1").build();
		hashMap = new HashMap<>();
		hashMap.put("a", "a1");
		hashMap.put("b", "b1");
		hashMap.put("c", "c1");

		adhocMap2 = factory.newMapBuilder(Set.of("a", "b", "c")).append("a1").append("b1").append("c1").build();
		hashMap2 = new HashMap<>(hashMap);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkAdhocMapVsHashMap.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	@Benchmark
	public boolean containsKey_adhoc() {
		return adhocMap.containsKey("a");
	}

	@Benchmark
	public boolean containsKey_hash() {
		return hashMap.containsKey("a");
	}

	@Benchmark
	public Object get_adhoc() {
		return adhocMap.get("a");
	}

	@Benchmark
	public Object get_hash() {
		return hashMap.get("a");
	}

	@Benchmark
	public int forEach_adhoc() {
		adhocMap.forEach((k, v) -> {
			length.addAndGet(k.length());
			length.addAndGet(v.toString().length());
		});

		return length.get();
	}

	@Benchmark
	public int forEach_hash() {
		hashMap.forEach((k, v) -> {
			length.addAndGet(k.length());
			length.addAndGet(v.toString().length());
		});

		return length.get();
	}

	@Benchmark
	public boolean equals_adhoc() {
		return adhocMap.equals(adhocMap2);
	}

	@Benchmark
	public boolean equals_hash() {
		return hashMap.equals(hashMap2);
	}

	@Benchmark
	public int compare_adhoc() {
		return adhocMap.compareTo(adhocMap2);
	}

	@Benchmark
	public int compare_hash() {
		return MapComparators.mapComparator().compare(hashMap, hashMap2);
	}

}
