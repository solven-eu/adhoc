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
package eu.solven.adhoc.engine.dag.fuser;

import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;

/**
 * Pluggable optimizer pass over an in-progress query-steps DAG. The {@code QueryStepsDagBuilder} runs the configured
 * optimizer after the DAG is fully populated and before {@code QueryStepsDag} is returned; the optimizer mutates
 * {@code multigraph} and {@code dag} in place.
 *
 * <p>
 * Implementations should not mutate {@code roots} or {@code stepToValue}: those are passed for read-only inspection (an
 * optimizer may need to know which steps are user-requested or pre-cached and therefore must not be removed).
 *
 * <p>
 * Implementations should be idempotent: running the same optimizer twice on the same DAG must produce the same result
 * as running it once.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IQueryStepsDagFuser {

	/**
	 * Apply this optimizer's rewrites to the in-progress DAG.
	 * 
	 * Explicits (roots) and leaves nodes must remain unchanged ; only the internal structure may change.
	 *
	 * @param multigraph
	 *            mutable multigraph (mutated in place)
	 * @param dag
	 *            mutable DAG view (mutated in place; must stay in sync with {@code multigraph})
	 * @param roots
	 *            user-requested steps; read-only. A step listed here must survive the optimization (its cuboid is
	 *            required by {@code toTabularView}).
	 * @param stepToValue
	 *            pre-loaded cache entries; read-only. A step keyed here was loaded from the cache and must not be
	 *            removed by the optimizer.
	 */
	void fuse(DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			IAdhocDag<CubeQueryStep> dag,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue);
}
