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
import java.util.List;
import java.util.Map;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Strings;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.column.navigable_else_hash.MultitypeNavigableElseHashColumn;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer.SplitTableQueries;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Benchmarks related with {@link TableQueryOptimizer}.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class TestBenchmarkTableQueryOptimizer extends ABenchmarkable {
	BenchmarkTableQueryOptimizerState state = new BenchmarkTableQueryOptimizerState();

	@Override
	public int nbIterations() {
		return 1;
	}

	@Value
	public static class BenchmarkTableQueryOptimizerState {
		ITableQueryOptimizer optimizer = new TableQueryOptimizerFactory().makeOptimizer(AdhocFactoriesUnsafe.factories);

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
		SplitTableQueries inducerAndInduced = SplitTableQueries.builder().inducedToInducer(dag).build();
		Map<CubeQueryStep, ISliceToValue> stepToValues = new LinkedHashMap<>();

		CubeQueryStep inducedGrandTotal = CubeQueryStep.builder().measure(Aggregator.sum("v")).build();
		CubeQueryStep inducedByA =
				CubeQueryStep.builder().measure(Aggregator.sum("v")).groupBy(GroupByColumns.named("a")).build();
		CubeQueryStep inducedByAB =
				CubeQueryStep.builder().measure(Aggregator.sum("v")).groupBy(GroupByColumns.named("a", "b")).build();
		CubeQueryStep inducedByB =
				CubeQueryStep.builder().measure(Aggregator.sum("v")).groupBy(GroupByColumns.named("b")).build();

		public void setup() {
			IAdhocGroupBy groupByABC = GroupByColumns.named("a", "b", "c");
			CubeQueryStep inducerABC = CubeQueryStep.builder().measure(Aggregator.sum("v")).groupBy(groupByABC).build();

			dag.addVertex(inducerABC);

			// 3 columns to 0
			dag.addVertex(inducedGrandTotal);
			dag.addEdge(inducedGrandTotal, inducerABC);

			// 3 columns to 1
			dag.addVertex(inducedByA);
			dag.addEdge(inducedByA, inducerABC);

			// 3 columns to 2
			dag.addVertex(inducedByAB);
			dag.addEdge(inducedByAB, inducerABC);

			// 3 columns to 1, but mis-ordered
			dag.addVertex(inducedByB);
			dag.addEdge(inducedByB, inducerABC);

			// inducerABC is navigable as we do nice inserts
			IMultitypeColumnFastGet<IAdhocSlice> inducerColumn =
					MultitypeNavigableElseHashColumn.<IAdhocSlice>builder().build();
			ISliceFactory mapFactory =
					AdhocFactoriesUnsafe.factories.getSliceFactoryFactory().makeFactory(IHasQueryOptions.noOption());

			int cardinalityPerKey = 16;
			int maxDigits = 3;

			List<String> keys = List.of("a", "b", "c");
			for (int iA = 0; iA < cardinalityPerKey; iA++) {
				// pad to ensure proper order
				String vA = "a" + Strings.padStart(Integer.toString(iA), maxDigits, '0');
				for (int iB = 0; iB < cardinalityPerKey; iB++) {
					String vB = "a" + Strings.padStart(Integer.toString(iB), maxDigits, '0');
					for (int iC = 0; iC < cardinalityPerKey; iC++) {
						String vC = "a" + Strings.padStart(Integer.toString(iC), maxDigits, '0');
						IAdhocSlice slice =
								mapFactory.newMapBuilder(keys).append(vA, "b" + vB, "c" + vC).build().asSlice();
						inducerColumn.append(slice).onLong(iA * iB * iC);
					}
				}
			}

			ISliceToValue inducerValues = SliceToValue.forGroupBy(() -> groupByABC).values(inducerColumn).build();
			stepToValues.put(inducerABC, inducerValues);

			log.info("inducerABC has {} slices", inducerColumn.size());
		}

		public IMultitypeMergeableColumn<IAdhocSlice> evaluateInduced(CubeQueryStep induced) {
			return optimizer.evaluateInduced(IHasQueryOptions.noOption(), inducerAndInduced, stepToValues, induced);
		}

	}

	@BeforeEach
	public void setup() {
		state.setup();
	}

	@Test
	public void fromABC_toGrandTotal() {
		doBenchmark(() -> state.evaluateInduced(state.getInducedGrandTotal()));
	}

	@Test
	public void fromABC_toA() {
		doBenchmark(() -> state.evaluateInduced(state.getInducedByA()));
	}

	@Test
	public void fromABC_toAB() {
		doBenchmark(() -> state.evaluateInduced(state.getInducedByAB()));
	}

	@Test
	public void fromABC_toB_misordered() {
		doBenchmark(() -> state.evaluateInduced(state.getInducedByB()));
	}

}
