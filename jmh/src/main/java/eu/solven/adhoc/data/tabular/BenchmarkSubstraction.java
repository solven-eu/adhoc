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

import java.util.Arrays;
import java.util.List;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.measure.sum.SubstractionCombination;
import eu.solven.adhoc.measure.transformator.ICombinationBinding;
import eu.solven.adhoc.measure.transformator.iterator.SlicedRecordFromSlices;

/**
 * Benchmarks related with {@link SubstractionCombination}.
 * 
 * @author Benoit Lacelle
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@SuppressWarnings("checkstyle:MagicNumber")
public class BenchmarkSubstraction {

	SubstractionCombination substraction = new SubstractionCombination();

	IValueProvider leftLong = IValueProvider.setValue(234L);
	IValueProvider rightLong = IValueProvider.setValue(123L);
	ISlicedRecord tabularRecordLong =
			SlicedRecordFromSlices.builder().valueProvider(leftLong).valueProvider(rightLong).build();

	List<?> arrayLong = Arrays.asList(234, 123);

	ICombinationBinding binding = substraction.bind(2);

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkSubstraction.class.getSimpleName()).forks(1).build();
		new Runner(opt).run();
	}

	@Benchmark
	public IValueProvider combineSlicedRecord_Long() {
		return substraction.combine(null, tabularRecordLong);
	}

	@Benchmark
	public Object combineArray_Long() {
		return substraction.combine(null, arrayLong);
	}

	@Benchmark
	public IValueProvider combineSlicedRecord_Long_binding() {
		return substraction.combine(binding, null, tabularRecordLong);
	}

}
