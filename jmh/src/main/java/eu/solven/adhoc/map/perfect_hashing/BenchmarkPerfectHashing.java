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
package eu.solven.adhoc.map.perfect_hashing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
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

import com.google.common.primitives.Ints;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;

/**
 * Benchmarks related with {@link IHasIndexOf<String>}, i.e. faster way to get the rank of a String in a
 * {@link NavigableSet}.
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
public class BenchmarkPerfectHashing {

	List<String> keys = List.of("a", "b", "c", "d", "e");

	private Object2IntFunction<String> sux4jHashing(Collection<String> keys) {

		try {
			return new GOVMinimalPerfectHashFunction.Builder<String>().keys(keys)
					.transform(TransformationStrategies.utf16())
					.build()
					.andThenInt(Ints::saturatedCast);
		} catch (IOException e) {
			throw new UncheckedIOException("Issue with " + keys, e);
		}
	}

	IHasIndexOf<String> guavaSet = ImmutableSortedSetHasIndexOf.builder().keys(keys).build();
	IHasIndexOf<String> guavaSetWithHashMap =
			CollectionWithCustomIndexOf.<String>builder().keys(keys).hashMap().build();
	IHasIndexOf<String> guavaSetWithPerfectHash = CollectionWithCustomIndexOf.<String>builder()
			.keys(keys)
			.factory(keys -> this.sux4jHashing(keys)::getInt)
			.build();
	IHasIndexOf<String> guavaSetWithSimplePerfectHashV2 =
			CollectionWithCustomIndexOf.<String>builder().keys(keys).unsafePerfectHash().build();

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkPerfectHashing.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	@Benchmark
	public int get_guava() {
		return guavaSet.indexOf("c");
	}

	@Benchmark
	public int get_guava_withHashMap() {
		return guavaSetWithHashMap.indexOf("c");
	}

	@Benchmark
	public int get_guava_withSux4J() {
		return guavaSetWithPerfectHash.indexOf("c");
	}

	@Benchmark
	public int get_guava_withSimplePerfectHash() {
		return guavaSetWithSimplePerfectHashV2.indexOf("c");
	}

	@Benchmark
	public int get_guava_withSimplePerfectHash_unsafe() {
		return guavaSetWithSimplePerfectHashV2.unsafeIndexOf("c");
	}

}
