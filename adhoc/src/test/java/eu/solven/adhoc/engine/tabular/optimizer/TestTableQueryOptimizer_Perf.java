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
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.IValueProviderTestHelpers;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer.SplitTableQueries;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestTableQueryOptimizer_Perf {

	int cardinalityIn = 1_000_000;
	int cardinalityOut = 1000;

	TableQueryOptimizer optimizer = new TableQueryOptimizer(AdhocFactories.builder().build());
	DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
	Map<CubeQueryStep, ISliceToValue> inducers = new LinkedHashMap<>();

	@Test
	public void testReduce() {
		Aggregator agg = Aggregator.sum("k1");

		CubeQueryStep inducerStep =
				CubeQueryStep.builder().measure(agg).groupBy(GroupByColumns.named("c0", "c1")).build();
		CubeQueryStep inducedStep = CubeQueryStep.edit(inducerStep).groupBy(GroupByColumns.named("c0")).build();
		dag.addVertex(inducerStep);
		dag.addVertex(inducedStep);
		dag.addEdge(inducedStep, inducerStep);

		SplitTableQueries split =
				SplitTableQueries.builder().inducer(inducerStep).induced(inducedStep).dagToDependancies(dag).build();

		IMultitypeColumnFastGet<SliceAsMap> inducerValues = MultitypeHashColumn.<SliceAsMap>builder().build();

		NavigableSet<String> inColumns = new TreeSet<>(ImmutableSet.of("c0", "c1"));
		IntStream.range(0, cardinalityIn).forEach(rowIndex -> {
			inducerValues
					.append(SliceAsMap.fromMap(
							AdhocMap.builder(inColumns).append(rowIndex % cardinalityOut).append(rowIndex).build()))
					.onLong(rowIndex);
		});

		ISliceToValue inducerValues2 = SliceToValue.builder().columns(Set.of("c0", "c1")).values(inducerValues).build();
		inducers.put(inducerStep, inducerValues2);

		IMultitypeMergeableColumn<SliceAsMap> induced =
				optimizer.evaluateInduced(() -> Set.of(), split, inducers, inducedStep);

		Assertions.assertThat(induced.size()).isEqualTo(cardinalityOut);

		long value0 = IntStream.rangeClosed(0, (cardinalityIn - 1) / cardinalityOut)
				.mapToLong(i -> i * cardinalityOut + 0)
				.sum();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(induced.onValue(SliceAsMap.fromMap(Map.of("c0", 0)))))
				.isEqualTo(value0);

		long value1 = IntStream.rangeClosed(0, (cardinalityIn - 2) / cardinalityOut)
				.mapToLong(i -> i * cardinalityOut + 1)
				.sum();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(induced.onValue(SliceAsMap.fromMap(Map.of("c0", 1)))))
				.isEqualTo(value1);
	}
}
