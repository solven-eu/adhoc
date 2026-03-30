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
package eu.solven.adhoc.engine.tabular.splitter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphHelpers;
import eu.solven.adhoc.engine.tabular.optimizer.IAdhocDag;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.stripper.IFilterStripper;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.query.cube.IGroupBy;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Hold the logic for {@link InduceByAdhocComplete}.
 *
 * @author Benoit Lacelle
 */
@Slf4j
@RequiredArgsConstructor
public class InduceByAdhocCompleteInner {

	@NonNull
	final IFilterStripperFactory stripperFactory;

	/**
	 * Builds all inducing edges for a group of steps sharing the same measure and context into a fresh local
	 * {@link IAdhocDag}. Uses two pre-computed indices to avoid the O(n²) cross-product of expensive filter and groupBy
	 * comparisons:
	 * <ul>
	 * <li>GroupBy containment index: for each distinct induced {@link IGroupBy}, the distinct inducer {@link IGroupBy}s
	 * whose columns are a superset — computed once per distinct groupBy pair (O(g²)).</li>
	 * <li>Filter strictness index: for each distinct induced filter, the set of inducer filters that are laxer —
	 * computed once per distinct filter pair (O(f²)).</li>
	 * </ul>
	 * Only {@link InducerHelpers#makeLeftoverFilter} must still be checked per step pair (it depends on both the
	 * inducer groupBy columns and the filter pair simultaneously), but it is reached only when both indices agree.
	 *
	 * <p>
	 * This method is thread-safe: it operates exclusively on a new local graph and its inputs are read-only.
	 * 
	 * @param sharedStripper
	 */
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	public IAdhocDag<TableQueryStep> buildEdgesForContextGroupLocal(List<TableQueryStep> contextSteps) {
		IAdhocDag<TableQueryStep> localDag = GraphHelpers.makeGraph();

		// Phase 2: groupBy containment index — O(g²) for g distinct groupBys in this context group.
		// For each distinct induced groupBy, collect all distinct groupBys that are supersets (valid inducers).
		Map<IGroupBy, List<IGroupBy>> inducedGroupByToInducerGroupBys =
				makeInducedGroupByToInducerGroupBys(contextSteps);

		// Phase 3: filter strictness index — O(f²) for f distinct filters in this context group.
		// For each distinct induced filter, pre-compute the set of inducer filters that are laxer.
		Map<ISliceFilter, Set<ISliceFilter>> inducedFilterToLaxerInducerFilters =
				makeInducedFilterToLaxerInducedFilters(contextSteps);

		// Phase 4: index steps by groupBy for O(1) candidate lookup
		Map<IGroupBy, List<TableQueryStep>> groupByToSteps =
				contextSteps.stream().collect(Collectors.groupingBy(TableQueryStep::getGroupBy));

		// Phase 5: for each induced step, use the indices to enumerate only valid inducer candidates
		contextSteps.forEach(induced -> {
			localDag.addVertex(induced);

			ISliceFilter inducedFilter = induced.getFilter();
			if (ISliceFilter.MATCH_NONE.equals(inducedFilter)) {
				// MATCH_NONE steps are independent: they do not wait for any inducer
				return;
			}

			Set<ISliceFilter> validInducerFilters = inducedFilterToLaxerInducerFilters.get(inducedFilter);
			List<IGroupBy> validInducerGroupBys = inducedGroupByToInducerGroupBys.get(induced.getGroupBy());

			validInducerGroupBys.forEach(inducerGroupBy -> {
				Collection<IAdhocColumn> inducerColumns = inducerGroupBy.getColumns();
				groupByToSteps.get(inducerGroupBy)
						.stream()
						// No edge from a step to itself
						.filter(inducer -> inducer != induced)
						// Inducer filter must be laxer (pre-computed per distinct filter pair)
						.filter(inducer -> validInducerFilters.contains(inducer.getFilter()))
						// Leftover filter must be expressible using only inducer groupBy columns
						.filter(inducer -> InducerHelpers
								.makeLeftoverFilter(inducerColumns, inducer.getFilter(), inducedFilter)
								.isPresent())
						.forEach(inducer -> {
							localDag.addVertex(inducer);
							localDag.addEdge(induced, inducer);
							log.debug("Induced -> Inducer ({} -> {})", induced, inducer);
						});
			});
		});

		return localDag;
	}

	protected Map<IGroupBy, List<IGroupBy>> makeInducedGroupByToInducerGroupBys(List<TableQueryStep> contextSteps) {
		List<IGroupBy> distinctGroupBys =
				contextSteps.stream().map(TableQueryStep::getGroupBy).distinct().collect(Collectors.toList());

		Map<IGroupBy, List<IGroupBy>> inducedGroupByToInducerGroupBys = new HashMap<>();
		for (IGroupBy gbInduced : distinctGroupBys) {
			Collection<IAdhocColumn> inducedCols = gbInduced.getColumns();
			List<IGroupBy> inducerGroupBys = new ArrayList<>();
			for (IGroupBy gbInducer : distinctGroupBys) {
				if (gbInducer.getColumns().containsAll(inducedCols)) {
					inducerGroupBys.add(gbInducer);
				}
			}
			inducedGroupByToInducerGroupBys.put(gbInduced, inducerGroupBys);
		}
		return inducedGroupByToInducerGroupBys;
	}

	protected Map<ISliceFilter, Set<ISliceFilter>> makeInducedFilterToLaxerInducedFilters(
			List<TableQueryStep> contextSteps) {
		ImmutableSet<ISliceFilter> distinctFilters =
				contextSteps.stream().map(TableQueryStep::getFilter).collect(ImmutableSet.toImmutableSet());

		Map<ISliceFilter, Set<ISliceFilter>> inducedFilterToLaxerInducerFilters = new LinkedHashMap<>();
		for (ISliceFilter fInduced : distinctFilters) {
			if (ISliceFilter.MATCH_NONE.equals(fInduced)) {
				// MATCH_NONE steps are independent: no inducer can serve them
				continue;
			}
			IFilterStripper stripper = stripperFactory.makeFilterStripper(fInduced);
			Set<ISliceFilter> laxer = new LinkedHashSet<>();
			for (ISliceFilter fInducer : distinctFilters) {
				if (stripper.isStricterThan(fInducer)) {
					laxer.add(fInducer);
				}
			}
			inducedFilterToLaxerInducerFilters.put(fInduced, laxer);
		}
		return inducedFilterToLaxerInducerFilters;
	}
}
