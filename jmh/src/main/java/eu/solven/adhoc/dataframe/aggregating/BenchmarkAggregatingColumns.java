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
package eu.solven.adhoc.dataframe.aggregating;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.cuboid.slice.SliceHelpers;
import eu.solven.adhoc.dataframe.tabular.primitives.Object2IntBiConsumer;
import eu.solven.adhoc.measure.model.Aggregator;
import it.unimi.dsi.fastutil.objects.AbstractObject2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import lombok.extern.slf4j.Slf4j;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Benchmarks related with {@link AggregatingColumns}.
 * 
 * @author Benoit Lacelle
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Slf4j
@SuppressWarnings("checkstyle:MagicNumber")
public class BenchmarkAggregatingColumns {

	int size = 1_000_000;

	Object2IntMap<ISlice> sliceToIndex;

	AggregatingColumns<String> columns = AggregatingColumns.<String>builder().build();

	Aggregator countAsterisk = Aggregator.countAsterisk();

	@Setup
	public void setup() {
		sliceToIndex = new Object2IntOpenHashMap<>(size);

		for (int i = 0; i < size; i++) {
			sliceToIndex.put(SliceHelpers.asSlice(Map.of("c", "c1", "row_index", i)), sliceToIndex.size());
		}
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkAggregatingColumns.class.getSimpleName()).forks(1).build();
		new Runner(opt).run();
	}


	// visible for benchmarks
	@SuppressWarnings("PMD.LooseCoupling")
	@Deprecated(since = "Not used anymore")
	public static <T extends Comparable<T>> ObjectArrayList<Object2IntMap.Entry<T>> doSort(
			Consumer<Object2IntBiConsumer<T>> sliceToIndex,
			int size) {
		log.debug("> sorting {}", size);

		// Do not rely on a TreeMap, else the sorting is done one element at a time
		// ObjectArrayList enables calling `Arrays.parallelSort`
		// `.wrap` else will rely on a `Object[]`, which will later fail on `Arrays.parallelSort`
		ObjectArrayList<Object2IntMap.Entry<T>> sortedEntries = ObjectArrayList.wrap(new Object2IntMap.Entry[size], 0);

		sliceToIndex.accept(
				(slice, rowIndex) -> sortedEntries.add(new AbstractObject2IntMap.BasicEntry<>(slice, rowIndex)));

		Arrays.parallelSort(sortedEntries.elements(), Map.Entry.comparingByKey());

		log.debug("< sorting {}", size);

		return sortedEntries;
	}

	// @Benchmark
	public ObjectArrayList<Object2IntMap.Entry<ISlice>> sort() {
		return BenchmarkAggregatingColumns.<ISlice>doSort(c -> {
			sliceToIndex.forEach(c::acceptObject2Int);
		}, sliceToIndex.size());
	}

	@Benchmark
	public AggregatingColumns<String> contribute() {
		for (int i = 0; i < size; i++) {
			columns.contribute(Integer.toString(i), countAsterisk);
		}

		return columns;
	}

	// One slow step of `contribute` is the `dictionarize` step
	@Benchmark
	public AggregatingColumns<String> contribute_only_dictionarize() {
		for (int i = 0; i < size; i++) {
			columns.dictionarize(Integer.toString(i));
		}

		return columns;
	}

}
