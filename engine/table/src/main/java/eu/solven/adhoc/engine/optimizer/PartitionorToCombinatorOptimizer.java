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
package eu.solven.adhoc.engine.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Partitionor;
import lombok.extern.slf4j.Slf4j;

/**
 * Rewrites {@link Partitionor}-measure steps as equivalent {@link Combinator}-measure steps when the step's groupBy
 * already covers the Partitionor's own groupBy. In that case the Partitionor degenerates to its embedded combination:
 * each partition contains exactly one slice (so the per-partition aggregation is a no-op), and the engine routes the
 * underlyings at the same granularity a Combinator would request.
 *
 * <p>
 * Intended to run BEFORE {@link FoldCombinatorSubgraphsOptimizer} in a {@link CompositeQueryStepsDagOptimizer}: the
 * rewritten Combinators become eligible for chain / subgraph folding.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class PartitionorToCombinatorOptimizer implements IQueryStepsDagOptimizer {

	@Override
	public void optimize(DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			IAdhocDag<CubeQueryStep> dag,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue) {
		List<CubeQueryStep> candidates = new ArrayList<>(multigraph.vertexSet());
		for (CubeQueryStep step : candidates) {
			if (!(step.getMeasure() instanceof Partitionor partitionor)) {
				continue;
			}
			if (roots.contains(step)) {
				continue;
			}
			if (stepToValue.containsKey(step)) {
				continue;
			}
			if (!step.getGroupBy().getSortedColumns().containsAll(partitionor.getGroupBy().getSortedColumns())) {
				continue;
			}

			Combinator.CombinatorBuilder builder = Combinator.builder()
					.name(partitionor.getName())
					.combinationKey(partitionor.getCombinationKey())
					.tags(partitionor.getTags());
			for (String underlying : partitionor.getUnderlyings()) {
				builder.underlying(underlying);
			}
			for (Map.Entry<String, ?> opt : partitionor.getCombinationOptions().entrySet()) {
				builder.combinationOption(opt.getKey(), opt.getValue());
			}
			OptimizerHelpers.replaceStepMeasure(multigraph, dag, step, builder.build());

			log.debug("Rewrote Partitionor step {} as Combinator", partitionor.getName());
		}
	}
}
