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

import java.util.Map;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.measure.model.Aggregator;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
public class BenchmarkAggregatingColumns {

	int size = 1_000_000;

	Object2IntMap<SliceAsMap> sliceToIndex;

	AggregatingColumnsV2<String> columns = AggregatingColumnsV2.<String>builder().build();

	Aggregator countAsterisk = Aggregator.countAsterisk();

	@Setup
	public void setup() {
		sliceToIndex = new Object2IntOpenHashMap<>(size);

		for (int i = 0; i < size; i++) {
			sliceToIndex.put(SliceAsMap.fromMap(Map.of("c", "c1", "row_index", i)), sliceToIndex.size());
		}
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(BenchmarkAggregatingColumns.class.getSimpleName()).forks(1).build();
		new Runner(opt).run();
	}

	// @Benchmark
	public ObjectArrayList<Object2IntMap.Entry<SliceAsMap>> sort() {
		return AggregatingColumnsV2.<SliceAsMap>doSort(c -> {
			sliceToIndex.forEach((slice, index) -> c.acceptObject2Int(slice, index));
		}, sliceToIndex.size());
	}

	@Benchmark
	public AggregatingColumnsV2<String> contribute() {
		for (int i = 0; i < size; i++) {
			columns.contribute(Integer.toString(i), countAsterisk);
		}

		return columns;
	}

	// One slow step of `contribute` is the `dictionarize` step
	@Benchmark
	public AggregatingColumnsV2<String> contribute_only_dictionarize() {
		for (int i = 0; i < size; i++) {
			columns.dictionarize(Integer.toString(i));
		}

		return columns;
	}

}
