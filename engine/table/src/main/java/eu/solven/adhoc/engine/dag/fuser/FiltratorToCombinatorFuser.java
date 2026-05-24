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
import java.util.Objects;
import java.util.Set;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.AdhocFilterUnsafe;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Filtrator;
import eu.solven.adhoc.model.measure.IMeasure;
import lombok.extern.slf4j.Slf4j;

/**
 * Rewrites {@link Filtrator}-measure steps as equivalent passthrough {@link Combinator}-measure steps when the
 * Filtrator's filter is already implied by the step's own filter — i.e. when
 * {@code step.filter AND filtrator.filter == step.filter} after optimization. In that case
 * {@code FiltratorQueryStep.getUnderlyingSteps()} would build its underlying at the same filter as the step itself, and
 * {@code produceOutputColumn} just runs {@link CoalesceCombination} (passthrough) — exactly what a Combinator with
 * {@code combinationKey=COALESCE} does.
 *
 * <p>
 * Intended to run BEFORE {@link CombinatorSubgraphsFuser} in a {@link CompositeDagFuser}: the resulting passthrough
 * Combinators participate in chain / subgraph folding, so a redundant Filtrator no longer breaks the foldability of its
 * surroundings.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class FiltratorToCombinatorFuser extends AToCombinatorFuser {

	@Override
	protected boolean isCandidate(CubeQueryStep step,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue) {
		return step.getMeasure() instanceof Filtrator filtrator && !roots.contains(step)
				&& !stepToValue.containsKey(step)
				&& filterIsRedundant(step.getFilter(), filtrator.getFilter());
	}

	@Override
	protected Combinator buildReplacement(IMeasure original) {
		Filtrator filtrator = (Filtrator) original;
		return Combinator.builder()
				.name(filtrator.getName())
				.underlying(filtrator.getUnderlying())
				.combinationKey(CoalesceCombination.KEY)
				.tags(filtrator.getTags())
				.build();
	}

	@Override
	protected void logRewrite(IMeasure original) {
		log.debug("Rewrote Filtrator step {} as passthrough Combinator (filter already implied)", original.getName());
	}

	/**
	 * @return true iff {@code filtratorFilter} is implied by {@code stepFilter} — equivalently,
	 *         {@code stepFilter AND filtratorFilter == stepFilter} once optimized. Trivial fast path: a matchAll
	 *         Filtrator filter is always redundant.
	 */
	protected boolean filterIsRedundant(ISliceFilter stepFilter, ISliceFilter filtratorFilter) {
		if (filtratorFilter.isMatchAll()) {
			return true;
		}
		IFilterOptimizer optimizer = AdhocFilterUnsafe.filterOptimizer;
		ISliceFilter combined = FilterBuilder.and(stepFilter, filtratorFilter).optimize(optimizer);
		return Objects.equals(combined, stepFilter);
	}
}
