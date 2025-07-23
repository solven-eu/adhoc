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
package eu.solven.adhoc.query.filter;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.google.common.base.Predicates;

import eu.solven.adhoc.util.AdhocCollectionHelpers;

/**
 * Benchmarks different implementations unnesting a {@link Collection} into a {@link List}.
 * <p>
 * 2025-07-04: Curiously, it appears `unnestWithStream` is faster when there is many nested Collections
 *
 * @author Benoit Lacelle
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@SuppressWarnings("checkstyle:MagicNumber")
public class BenchmarkCollectionUnnesting {

	final List<?> noCollection = Arrays.asList(123, 12.34, "foo", LocalDate.now());

	final List<?> someCollections =
			Arrays.asList(123, Arrays.asList(12.34, Arrays.asList(Arrays.asList("foo"), LocalDate.now())));

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkCollectionUnnesting.class.getSimpleName()).forks(1).build();
		new Runner(opt).run();
	}

	private List<?> unnestWithStream(Collection<?> c) {
		return c.stream().flatMap(e -> {
			if (e instanceof Collection<?> nestedC) {
				return nestedC.stream();
			} else {
				return Stream.of(e);
			}
		}).toList();
	}

	@Benchmark
	public Collection<?> unnestAsList_noCollection() {
		return AdhocCollectionHelpers.unnestAsCollection(noCollection);
	}

	@Benchmark
	public Collection<?> unnestAsList_someCollections() {
		return AdhocCollectionHelpers.unnestAsCollection(someCollections);
	}

	@Benchmark
	public List<?> unnestAsList2_noCollection() {
		return AdhocCollectionHelpers.unnestAsList(noCollection, Predicates.alwaysTrue());
	}

	@Benchmark
	public List<?> unnestAsList2_someCollections() {
		return AdhocCollectionHelpers.unnestAsList(someCollections, Predicates.alwaysTrue());
	}

	@Benchmark
	public List<?> stream_noCollection() {
		return unnestWithStream(noCollection);
	}

	@Benchmark
	public List<?> stream_someCollections() {
		return unnestWithStream(someCollections);
	}

}
