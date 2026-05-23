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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;

/**
 * Runs a sequence of {@link IQueryStepsDagOptimizer}s in order. Useful when several independent rewrites should apply
 * to the same DAG; each delegate sees the output of the previous one.
 *
 * <p>
 * Composition order matters: a later optimizer can take advantage of earlier ones. For example, a
 * {@code PartitionorToCombinatorOptimizer} (turning a Partitionor whose partitioning columns are already in the groupBy
 * into a plain Combinator) should run BEFORE {@link FoldCombinatorSubgraphsOptimizer} so the newly-introduced
 * Combinators participate in the chain folding.
 *
 * @author Benoit Lacelle
 */
public class CompositeQueryStepsDagOptimizer implements IQueryStepsDagOptimizer {

	private final List<IQueryStepsDagOptimizer> delegates;

	public CompositeQueryStepsDagOptimizer(IQueryStepsDagOptimizer... delegates) {
		this(Arrays.asList(delegates));
	}

	public CompositeQueryStepsDagOptimizer(List<? extends IQueryStepsDagOptimizer> delegates) {
		this.delegates = ImmutableList.copyOf(delegates);
	}

	@Override
	public void optimize(DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			IAdhocDag<CubeQueryStep> dag,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue) {
		for (IQueryStepsDagOptimizer delegate : delegates) {
			delegate.optimize(multigraph, dag, roots, stepToValue);
		}
	}
}
