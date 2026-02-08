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
package eu.solven.adhoc.map.factory;

import java.util.List;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.compression.ColumnarSliceFactory;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.map.IAdhocMap;

/**
 * Benchmarks related with {@link SliceAsMap#compareTo(SliceAsMap)}
 * 
 * @author Benoit Lacelle
 */
@SuppressWarnings({ "checkstyle:MemberName", "checkstyle:MagicNumber" })
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
public class BenchmarkColumnarSliceFactory {

	ColumnarSliceFactory factory = ColumnarSliceFactory.builder().build();

	Map<String, ?> javaMap = Map.of("a", "a1", "b", "b1");
	Map<String, ?> guavaMap = ImmutableMap.of("a", "a1", "b", "b1");

	Map<String, ?> adhocMap = factory.newMapBuilder(List.of("a", "b")).append("a1").append("b1").build();
	IAdhocMap adhocBeforeRetain = factory.newMapBuilder(List.of("a", "b", "c", "d"))
			.append("a1")
			.append("b1")
			.append("c1")
			.append("d1")
			.build();
	Map<String, ?> adhocRetained = adhocBeforeRetain.retainAll(ImmutableSet.of("a", "b"));

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkColumnarSliceFactory.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	// ---- .ctor() ----

	@Benchmark
	public Map<String, String> ctor_java() {
		return Map.of("a", "a1", "b", "b1");
	}

	@Benchmark
	public ImmutableMap<String, String> ctor_guava() {
		return ImmutableMap.of("a", "a1", "b", "b1");
	}

	@Benchmark
	public IMapBuilderPreKeys ctor_adhoc_2entries_prepare() {
		return factory.newMapBuilder(ImmutableList.of("a", "b")).append("a1").append("b1");
	}

	@Benchmark
	public IAdhocMap ctor_adhoc_2entries_prepare_build() {
		return factory.newMapBuilder(ImmutableList.of("a", "b")).append("a1").append("b1").build();
	}

	@Benchmark
	public IMapBuilderPreKeys ctor_adhoc_4entries_prepare() {
		return factory.newMapBuilder(ImmutableList.of("a", "b", "c", "d"))
				.append("a1")
				.append("b1")
				.append("c1")
				.append("d1");
	}

	@Benchmark
	public IAdhocMap ctor_adhoc_4entries_prepare_build() {
		return factory.newMapBuilder(ImmutableList.of("a", "b", "c", "d"))
				.append("a1")
				.append("b1")
				.append("c1")
				.append("d1")
				.build();
	}

	@Benchmark
	public IAdhocMap ctor_adhoc_retained2entries() {
		return adhocBeforeRetain.retainAll(ImmutableSet.of("a", "b"));
	}

	@Benchmark
	public IAdhocMap ctor_adhoc_retained2entries_fromScratch() {
		return factory.newMapBuilder(ImmutableList.of("a", "b", "c", "d"))
				.append("a1")
				.append("b1")
				.append("c1")
				.append("d1")
				.build()
				.retainAll(ImmutableSet.of("a", "b"));
	}

	// ---- .hashcode() ----

	// not cached
	@Benchmark
	public int hashcode_java() {
		return javaMap.hashCode();
	}

	// not cached
	@Benchmark
	public int hashcode_guava() {
		return guavaMap.hashCode();
	}

	// cached
	@Benchmark
	public int hashcode_adhoc() {
		return adhocMap.hashCode();
	}

	// cached
	@Benchmark
	public int hashcode_adhoc_retained() {
		return adhocRetained.hashCode();
	}

}
