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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSortedSet;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.cuboid.slice.SliceHelpers;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.inducer.ITableQueryInducer;
import eu.solven.adhoc.engine.tabular.inducer.TableQueryInducer;
import eu.solven.adhoc.map.factory.RowSliceFactory;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.primitive.IValueProviderTestHelpers;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestTableQueryFactory_Perf {

	int cardinalityIn = 1_000_000;
	int cardinalityOut = 1000;

	ITableQueryInducer inducer = new TableQueryInducer(AdhocFactories.builder().build());
	IAdhocDag<TableQueryStep> inducedToInducer = new AdhocDag<>();
	Map<TableQueryStep, ICuboid> inducers = new LinkedHashMap<>();

	RowSliceFactory factory = RowSliceFactory.builder().build();

	@Test
	public void testReduce() {
		Aggregator agg = Aggregator.sum("k1");

		TableQueryStep inducerStep =
				TableQueryStep.builder().aggregator(agg).groupBy(GroupByColumns.named("c0", "c1")).build();
		TableQueryStep inducedStep = TableQueryStep.edit(inducerStep).groupBy(GroupByColumns.named("c0")).build();
		inducedToInducer.addVertex(inducerStep);
		inducedToInducer.addVertex(inducedStep);
		inducedToInducer.addEdge(inducedStep, inducerStep);

		SplitTableQueries split = SplitTableQueries.builder().inducedToInducer(inducedToInducer).build();

		IMultitypeColumnFastGet<ISlice> inducerValues = MultitypeHashColumn.<ISlice>builder().build();

		// BEWARE This tests demonstrates DictionaryFactoryValue is behaving badly on large cardinalities
		RowSliceFactory sliceFactory = RowSliceFactory.builder().build();

		NavigableSet<String> inColumns = ImmutableSortedSet.of("c0", "c1");
		IntStream.range(0, cardinalityIn).forEach(rowIndex -> {
			inducerValues.append(SliceHelpers.asSlice(sliceFactory,
					factory.newMapBuilder(inColumns).append(rowIndex % cardinalityOut).append(rowIndex).build()))
					.onLong(rowIndex);
		});

		ICuboid inducerValues2 = Cuboid.builder().columns(Set.of("c0", "c1")).values(inducerValues).build();
		inducers.put(inducerStep, inducerValues2);

		IMultitypeMergeableColumn<ISlice> induced =
				inducer.evaluateInduced(IHasQueryOptions.noOption(), split, inducers, inducedStep);

		Assertions.assertThat(induced.size()).isEqualTo(cardinalityOut);

		long value0 = IntStream.rangeClosed(0, (cardinalityIn - 1) / cardinalityOut)
				.mapToLong(i -> i * cardinalityOut + 0)
				.sum();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(induced.onValue(SliceHelpers.asSlice(Map.of("c0", 0)))))
				.isEqualTo(value0);

		long value1 = IntStream.rangeClosed(0, (cardinalityIn - 2) / cardinalityOut)
				.mapToLong(i -> i * cardinalityOut + 1)
				.sum();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(induced.onValue(SliceHelpers.asSlice(Map.of("c0", 1)))))
				.isEqualTo(value1);
	}
}
