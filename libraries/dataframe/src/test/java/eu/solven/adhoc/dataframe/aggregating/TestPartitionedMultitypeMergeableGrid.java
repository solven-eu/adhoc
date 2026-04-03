/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;

public class TestPartitionedMultitypeMergeableGrid {
	AggregatingColumns<String> p1 = AggregatingColumns.<String>builder().build();
	AggregatingColumns<String> p2 = AggregatingColumns.<String>builder().build();

	PartitionedMultitypeMergeableGrid<String, Integer> partitioned =
			PartitionedMultitypeMergeableGrid.<String, Integer>builder().partition(p1).partition(p2).build();

	Aggregator sum = Aggregator.sum("k1");

	@Test
	public void testEmpty() {
		Assertions.assertThat(partitioned.getAggregators()).isEmpty();
		Assertions.assertThat(partitioned.getNbPartitions()).isEqualTo(2);

		Assertions.assertThat(partitioned)
				.hasToString(
						"PartitionedMultitypeMergeableGrid{nbPartitions=2, partition:0=AggregatingColumns{#slices=0, aggregators=0}, partition:1=AggregatingColumns{#slices=0, aggregators=0}}");
	}

	@Test
	public void testInsertSingleRow() {
		partitioned.openSlice("a1").contribute(sum).onLong(123);

		Assertions.assertThat(partitioned.getAggregators()).containsExactly("k1");

		Assertions.assertThat(partitioned)
				.hasToString(
						"PartitionedMultitypeMergeableGrid{nbPartitions=2, partition:0=AggregatingColumns{#slices=1, aggregators=1, a1={k1=123}}, partition:1=AggregatingColumns{#slices=0, aggregators=0}}");

		IMultitypeColumnFastGet<String> column =
				partitioned.closeColumn(CubeQueryStep.builder().measure("k1").build(), sum);

		Assertions.assertThat(column)
				.hasToString(
						"PartitionedColumn{nbPartitions=2, partition:0=a1=123, partition:1=MultitypeHashColumn{}}");
	}
}
