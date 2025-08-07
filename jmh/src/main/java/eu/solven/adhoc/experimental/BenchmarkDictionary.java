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
package eu.solven.adhoc.experimental;

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

import eu.solven.adhoc.data.row.slice.SliceAsMap;

/**
 * Benchmarks related with {@link SliceAsMap#compareTo(SliceAsMap)}
 *
 * @author Benoit Lacelle
 */
// https://java-performance.info/large-hashmap-overview-jdk-fastutil-goldman-sachs-hppc-koloboke-trove/
@SuppressWarnings({ "checkstyle:MemberName", "checkstyle:MagicNumber" })
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDictionary {

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkDictionary.class.getSimpleName())
				.forks(1)

				// https://jmh.morethan.io/
				.resultFormat(ResultFormatType.JSON)
				.result("jmh/target/" + System.currentTimeMillis() + ".json")

				.build();
		new Runner(opt).run();
	}

	IDictionary hashMap = new HashMapDictionary();
	IDictionary arrowDictionary = new HashMapDictionary();
	IDictionary adhocWithLock = new HashMapDictionary();
	IDictionary adhocLockFree = new HashMapDictionary();

	private IDictionary fill(IDictionary dictionary, int size) {
		for (int i = 0; i < size; i++) {
			dictionary.mapToInt(i);
		}

		return dictionary;
	}

	private IDictionary fillSmall(IDictionary dictionary) {
		return fill(dictionary, 16);
	}

	private IDictionary fillMedium(IDictionary dictionary) {
		return fill(dictionary, 16 * 16);
	}

	private IDictionary fillLarge(IDictionary dictionary) {
		return fill(dictionary, 16 * 16 * 16);
	}

	@Benchmark
	public IDictionary fillSmallHashmap() {
		return fillSmall(hashMap);
	}

	@Benchmark
	public IDictionary fillSmallArrow() {
		return fillSmall(arrowDictionary);
	}

	@Benchmark
	public IDictionary fillSmallAdhocWithLock() {
		return fillSmall(adhocWithLock);
	}

	@Benchmark
	public IDictionary fillSmallAdhocLockLess() {
		return fillSmall(adhocLockFree);
	}

	@Benchmark
	public IDictionary fillMediumHashmap() {
		return fillMedium(hashMap);
	}

	@Benchmark
	public IDictionary fillMediumArrow() {
		return fillMedium(arrowDictionary);
	}

	@Benchmark
	public IDictionary fillMediumAdhocWithLock() {
		return fillMedium(adhocWithLock);
	}

	@Benchmark
	public IDictionary fillMediumAdhocLockLess() {
		return fillMedium(adhocLockFree);
	}

	@Benchmark
	public IDictionary fillLargeHashmap() {
		return fillLarge(hashMap);
	}

	@Benchmark
	public IDictionary fillLargeArrow() {
		return fillLarge(arrowDictionary);
	}

	@Benchmark
	public IDictionary fillLargeAdhocWithLock() {
		return fillLarge(adhocWithLock);
	}

	@Benchmark
	public IDictionary fillLargeAdhocLockLess() {
		return fillLarge(adhocLockFree);
	}

}
