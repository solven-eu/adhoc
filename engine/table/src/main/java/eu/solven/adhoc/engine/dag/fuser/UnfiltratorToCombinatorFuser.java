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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Unfiltrator;
import lombok.extern.slf4j.Slf4j;

/**
 * Rewrites {@link Unfiltrator}-measure steps as equivalent passthrough {@link Combinator}-measure steps when the
 * Unfiltrator's filter-editor would leave the step's filter unchanged — i.e. when the step's filter has no clauses on
 * the columns the Unfiltrator would neutralise (Suppress mode) or already restricts only to listed columns (Retain
 * mode). In that case {@code UnfiltratorQueryStep.getUnderlyingSteps()} builds its underlying at the same filter as the
 * step itself, and {@code produceOutputColumn} returns the underlying cuboid verbatim — exactly what a Combinator with
 * {@code combinationKey=COALESCE} does.
 *
 * <p>
 * Intended to run BEFORE {@link CombinatorSubgraphsFuser} in a {@link CompositeDagFuser}: the
 * resulting passthrough Combinators participate in chain / subgraph folding.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class UnfiltratorToCombinatorFuser implements IQueryStepsDagFuser {

	@Override
	public void fuse(DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph,
			IAdhocDag<CubeQueryStep> dag,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue) {
		List<CubeQueryStep> candidates = new ArrayList<>(multigraph.vertexSet());
		for (CubeQueryStep step : candidates) {
			if (!(step.getMeasure() instanceof Unfiltrator unfiltrator)) {
				continue;
			}
			if (roots.contains(step)) {
				continue;
			}
			if (stepToValue.containsKey(step)) {
				continue;
			}
			if (!editorIsNoop(step.getFilter(), unfiltrator)) {
				continue;
			}

			Combinator passthrough = Combinator.builder()
					.name(unfiltrator.getName())
					.underlying(unfiltrator.getUnderlying())
					.combinationKey(CoalesceCombination.KEY)
					.tags(unfiltrator.getTags())
					.build();
			DagOptimizerHelpers.replaceStepMeasure(multigraph, dag, step, passthrough);

			log.debug("Rewrote Unfiltrator step {} as passthrough Combinator (editor is a no-op on step filter)",
					unfiltrator.getName());
		}
	}

	/**
	 * @return true iff applying the {@link Unfiltrator}'s filter editor to {@code stepFilter} would leave it unchanged.
	 *         Equivalent to: the step's filter has no clauses on any column the editor would neutralise. Trivially
	 *         {@code matchAll} survives any editor unchanged.
	 */
	protected boolean editorIsNoop(ISliceFilter stepFilter, Unfiltrator unfiltrator) {
		if (stepFilter.isMatchAll()) {
			return true;
		}
		IFilterEditor editor = switch (unfiltrator.getMode()) {
		case Suppress -> SimpleFilterEditor.suppressColumn(unfiltrator.getColumns());
		case Retain -> SimpleFilterEditor.retainsColumns(unfiltrator.getColumns());
		};
		ISliceFilter edited = editor.editFilter(stepFilter);
		return Objects.equals(edited, stepFilter);
	}
}
