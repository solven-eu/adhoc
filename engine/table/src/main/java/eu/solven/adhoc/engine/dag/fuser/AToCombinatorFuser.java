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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.IMeasure;
import lombok.extern.slf4j.Slf4j;

/**
 * Base class for fusers that rewrite individual measure-steps to an equivalent {@link Combinator}. Subclasses provide a
 * candidate predicate and a measure-to-Combinator transform; this class owns the shared orchestration: scanning the
 * input multigraph, defensively copying the graphs only when at least one candidate is found, applying the per-step
 * rewrite via {@link DagFuserHelpers#replaceStepMeasure}, and rebuilding the resulting {@link QueryStepsDag}.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public abstract class AToCombinatorFuser implements IQueryStepsDagFuser {

	@Override
	public final QueryStepsDag fuse(QueryStepsDag input) {
		List<CubeQueryStep> candidates = input.getMultigraph()
				.vertexSet()
				.stream()
				.filter(step -> isCandidate(step, input.getExplicits(), input.getStepToValues()))
				.toList();
		if (candidates.isEmpty()) {
			return input;
		}
		DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph =
				DagFuserHelpers.copyMultigraph(input.getMultigraph());
		IAdhocDag<CubeQueryStep> dag = DagFuserHelpers.copyDag(input.getInducedToInducer());
		candidates.forEach(step -> {
			IMeasure original = step.getMeasure();
			Combinator replacement = buildReplacement(original);
			DagFuserHelpers.replaceStepMeasure(multigraph, dag, step, replacement);
			logRewrite(original);
		});
		return input.toBuilder().multigraph(multigraph).inducedToInducer(dag).build();
	}

	/**
	 * @return true iff {@code step}'s measure should be rewritten by this fuser. Implementations should also verify
	 *         {@code step} is neither user-requested nor pre-cached.
	 */
	protected abstract boolean isCandidate(CubeQueryStep step,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue);

	/** Build the replacement {@link Combinator} for a candidate's current measure. */
	protected abstract Combinator buildReplacement(IMeasure original);

	/**
	 * Hook for the debug log emitted on each rewrite. The default prints {@code "Rewrote {classSimpleName} step {name}
	 * as Combinator"}; subclasses override to spell out <em>why</em> the rewrite is safe.
	 */
	protected void logRewrite(IMeasure original) {
		log.debug("Rewrote {} step {} as Combinator", original.getClass().getSimpleName(), original.getName());
	}
}
