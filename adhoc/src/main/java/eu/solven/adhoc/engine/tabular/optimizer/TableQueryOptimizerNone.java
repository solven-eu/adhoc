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

import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer.SplitTableQueries.SplitTableQueriesBuilder;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ITableQueryOptimizer} which does no optimization. It typically does one SQL per CubeQueryStep.
 * 
 * @author Benoit Lacelle
 */
// TODO Should this also drop the optimizations in `groupByEnablingFilterPerMeasure`?
@Slf4j
public class TableQueryOptimizerNone extends ATableQueryOptimizer {

	public TableQueryOptimizerNone(AdhocFactories factories, IFilterOptimizer filterOptimizer) {
		super(factories, filterOptimizer);
	}

	/**
	 * 
	 * @param tableQueries
	 * @return an Object partitioning TableQuery which can not be induced from those which can be induced.
	 */
	@Override
	public SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<CubeQueryStep> tableQueries) {
		if (tableQueries.isEmpty()) {
			return SplitTableQueries.empty();
		}

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer =
				new DirectedAcyclicGraph<>(DefaultEdge.class);

		SplitTableQueriesBuilder split = SplitTableQueries.builder();

		// Register all tableQueries as a vertex
		tableQueries.forEach(step -> {
			inducedToInducer.addVertex(step);
		});

		return split.inducedToInducer(inducedToInducer).build();
	}

}
