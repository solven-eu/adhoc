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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.map.factory.RowSliceFactory;

/**
 * Benchmarks related with {@link SliceAsMap#compareTo(SliceAsMap)}
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
public class BenchmarkAdhocMapFactory {

	RowSliceFactory factory = RowSliceFactory.builder().build();

	Supplier<Map<String, ?>> supplierJava = () -> {
		Map<String, Object> hashMap = HashMap.newHashMap(2);
		hashMap.put("a", "a1");
		hashMap.put("b", "b1");
		return hashMap;
	};
	Supplier<Map<String, ?>> supplierGuava = () -> ImmutableMap.of("a", "a1", "b", "b1");
	Supplier<Map<String, ?>> supplierAdhoc = () -> factory.newMapBuilder().put("a", "a1").put("b", "b1").build();
	Supplier<Map<String, ?>> supplierAdhocDisordered =
			() -> factory.newMapBuilder().put("b", "b1").put("a", "a1").build();

	Supplier<Map<String, ?>> supplierJava_b2 = () -> {
		Map<String, Object> hashMap = HashMap.newHashMap(2);
		hashMap.put("a", "a1");
		hashMap.put("b", "b2");
		return hashMap;
	};
	Supplier<Map<String, ?>> supplierGuava_b2 = () -> ImmutableMap.of("a", "a1", "b", "b2");
	Supplier<Map<String, ?>> supplierAdhoc_b2 = () -> factory.newMapBuilder().put("a", "a1").put("b", "b2").build();
	Supplier<Map<String, ?>> supplierAdhocDisordered_b2 =
			() -> factory.newMapBuilder().put("b", "b2").put("a", "a1").build();

	Map<String, ?> mapJava = supplierJava.get();
	Map<String, ?> mapGuava = supplierGuava.get();
	Map<String, ?> mapAdhoc = supplierAdhoc.get();
	Map<String, ?> mapAdhocDisordered = supplierAdhocDisordered.get();

	Map<String, ?> mapJava_bis = supplierJava.get();
	Map<String, ?> mapGuava_bis = supplierGuava.get();
	Map<String, ?> mapAdhoc_bis = supplierAdhoc.get();
	Map<String, ?> mapAdhocDisordered_bis = supplierAdhocDisordered.get();

	Map<String, ?> mapJava_b2 = supplierJava_b2.get();
	Map<String, ?> mapGuava_b2 = supplierGuava_b2.get();
	Map<String, ?> mapAdhoc_b2 = supplierAdhoc_b2.get();
	Map<String, ?> mapAdhocDisordered_b2 = supplierAdhocDisordered_b2.get();

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkAdhocMapFactory.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	// Constructors
	@Benchmark
	public Map<String, ?> build_java() {
		return supplierJava.get();
	}

	@Benchmark
	public Map<String, ?> build_guava() {
		return supplierGuava.get();
	}

	@Benchmark
	public Map<String, ?> build_adhoc() {
		return supplierAdhoc.get();
	}

	@Benchmark
	public Map<String, ?> build_adhoc_disordered() {
		return supplierAdhocDisordered.get();
	}

	// equals same implementation
	@Benchmark
	public boolean equals_same_javaVjava() {
		return mapJava.equals(mapJava_bis);
	}

	@Benchmark
	public boolean equals_same_guavaVguava() {
		return mapGuava.equals(mapGuava_bis);
	}

	@Benchmark
	public boolean equals_same_adhocVadhoc() {
		return mapAdhoc.equals(mapAdhoc_bis);
	}

	@Benchmark
	public boolean equals_same_adhocVadhocDisordered() {
		return mapAdhoc.equals(mapAdhocDisordered_bis);
	}

	@Benchmark
	public boolean equals_same_adhocDisorderedVadhocDisordered() {
		return mapAdhocDisordered.equals(mapAdhocDisordered_bis);
	}

	// not equals same implementation
	@Benchmark
	public boolean equals_diff_javaVjava() {
		return mapJava.equals(mapJava_b2);
	}

	@Benchmark
	public boolean equals_diff_guavaVguava() {
		return mapGuava.equals(mapGuava_b2);
	}

	@Benchmark
	public boolean equals_diff_adhocVadhoc() {
		return mapAdhoc.equals(mapAdhoc_b2);
	}

	@Benchmark
	public boolean equals_diff_adhocVadhocDisordered() {
		return mapAdhoc.equals(mapAdhocDisordered_b2);
	}

	@Benchmark
	public boolean equals_diff_adhocDisorderedVadhocDisordered() {
		return mapAdhocDisordered.equals(mapAdhocDisordered_b2);
	}

}
