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
import eu.solven.adhoc.engine.dag.GraphHelpers;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.FilterHelpers;
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
	 * <li>GroupBy containment index: for each distinct inducer {@link IGroupBy}, the distinct induced {@link IGroupBy}s
	 * whose columns are a subset — computed once per distinct groupBy pair (O(g²)).</li>
	 * <li>Filter strictness index: for each distinct inducer filter, the set of induced filters that are stricter —
	 * computed once per distinct filter pair (O(f²)) using a shared per-filter {@link IFilterStripper}.</li>
	 * </ul>
	 * The leftover-filter check (whether the extra filtering imposed by the induced step is expressible using only the
	 * inducer groupBy columns) is inlined in the inner loop and reuses the {@link IFilterStripper} already built for
	 * the outer inducer step, avoiding any additional {@code makeFilterStripper} calls.
	 *
	 * <p>
	 * This method is thread-safe: it operates exclusively on a new local graph and its inputs are read-only.
	 */
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	public IAdhocDag<TableQueryStep> buildEdgesForContextGroupLocal(List<TableQueryStep> contextSteps) {
		IAdhocDag<TableQueryStep> localDag = GraphHelpers.makeGraph();

		// Phase 2: groupBy containment index — O(g²) for g distinct groupBys in this context group.
		// For each distinct inducer groupBy, collect all distinct groupBys that are subsets (valid induceds).
		Map<IGroupBy, List<IGroupBy>> inducerGroupByToInducedGroupBys =
				makeInducerGroupByToInducedGroupBys(contextSteps);

		// Phase 3: one IFilterStripper per distinct filter, shared across both the strictness index and
		// the per-pair leftover check — no redundant makeFilterStripper calls.
		ImmutableSet<ISliceFilter> distinctFilters =
				contextSteps.stream().map(TableQueryStep::getFilter).collect(ImmutableSet.toImmutableSet());
		Map<ISliceFilter, IFilterStripper> filterToStripper = makeFilterToStripper(distinctFilters);

		// Phase 3b: filter strictness index — O(f²) for f distinct filters in this context group.
		// For each distinct inducer filter, pre-compute the set of induced filters that are stricter.
		Map<ISliceFilter, Set<ISliceFilter>> inducerFilterToStricterInducedFilters =
				makeInducerFilterToStricterInducedFilters(distinctFilters, filterToStripper);

		// Phase 4: index steps by groupBy for O(1) candidate lookup
		Map<IGroupBy, List<TableQueryStep>> groupByToSteps =
				contextSteps.stream().collect(Collectors.groupingBy(TableQueryStep::getGroupBy));

		// Phase 5: for each inducer step, use the indices to enumerate only valid induced candidates.
		// The inducer is the OUTER loop so its IFilterStripper (built in Phase 3) is fetched once and
		// reused for all induced candidates in the inner loop.
		contextSteps.forEach(inducer -> {
			localDag.addVertex(inducer);

			ISliceFilter inducerFilter = inducer.getFilter();
			if (ISliceFilter.MATCH_NONE.equals(inducerFilter)) {
				// MATCH_NONE steps produce no rows and cannot serve as inducer for any induced step
				return;
			}

			Set<ISliceFilter> validInducedFilters =
					inducerFilterToStricterInducedFilters.getOrDefault(inducerFilter, ImmutableSet.of());
			List<IGroupBy> validInducedGroupBys = inducerGroupByToInducedGroupBys.get(inducer.getGroupBy());

			// Fetched once per inducer — reused for all induced candidates below
			IFilterStripper inducerStripper = filterToStripper.get(inducerFilter);
			// Column-name set is also invariant across the inner loop
			ImmutableSet<String> inducerColumnNames = inducer.getGroupBy()
					.getColumns()
					.stream()
					.map(IAdhocColumn::getName)
					.collect(ImmutableSet.toImmutableSet());

			validInducedGroupBys.forEach(inducedGroupBy -> {
				groupByToSteps.get(inducedGroupBy)
						.stream()
						// No edge from a step to itself
						.filter(induced -> induced != inducer)
						// Induced filter must be stricter (pre-computed per distinct filter pair)
						.filter(induced -> validInducedFilters.contains(induced.getFilter()))
						// Leftover filter must be expressible using only inducer groupBy columns.
						// Inlined from InducerHelpers.makeLeftoverFilter, reusing inducerStripper
						// from the outer loop instead of creating a fresh one per pair.
						.filter(induced -> {
							ISliceFilter leftover = inducerStripper.strip(induced.getFilter());
							return inducerColumnNames.containsAll(FilterHelpers.getFilteredColumns(leftover));
						})
						.forEach(induced -> {
							localDag.addVertex(induced);
							localDag.addEdge(induced, inducer);
							log.debug("Induced -> Inducer ({} -> {})", induced, inducer);
						});
			});
		});

		return localDag;
	}

	/**
	 * Builds the groupBy containment index: for each distinct inducer {@link IGroupBy}, the list of distinct
	 * {@link IGroupBy}s whose column set is a subset (valid induced groupBys).
	 *
	 * @param contextSteps
	 *            all steps in the current context group
	 * @return map from inducer groupBy to the list of compatible induced groupBys
	 */
	protected Map<IGroupBy, List<IGroupBy>> makeInducerGroupByToInducedGroupBys(List<TableQueryStep> contextSteps) {
		List<IGroupBy> distinctGroupBys =
				contextSteps.stream().map(TableQueryStep::getGroupBy).distinct().collect(Collectors.toList());

		Map<IGroupBy, List<IGroupBy>> inducerGroupByToInducedGroupBys = new HashMap<>();
		for (IGroupBy gbInducer : distinctGroupBys) {
			Collection<IAdhocColumn> inducerCols = gbInducer.getColumns();
			List<IGroupBy> inducedGroupBys = new ArrayList<>();
			for (IGroupBy gbInduced : distinctGroupBys) {
				if (inducerCols.containsAll(gbInduced.getColumns())) {
					inducedGroupBys.add(gbInduced);
				}
			}
			inducerGroupByToInducedGroupBys.put(gbInducer, inducedGroupBys);
		}
		return inducerGroupByToInducedGroupBys;
	}

	/**
	 * Builds one {@link IFilterStripper} per distinct filter (excluding {@link ISliceFilter#MATCH_NONE}), keyed by the
	 * filter itself. The same stripper instance is reused for both the filter-strictness index and the per-pair
	 * leftover check, avoiding redundant {@code makeFilterStripper} calls.
	 *
	 * @param distinctFilters
	 *            all distinct filters present in the context group
	 * @return map from filter to its pre-built stripper
	 */
	protected Map<ISliceFilter, IFilterStripper> makeFilterToStripper(ImmutableSet<ISliceFilter> distinctFilters) {
		Map<ISliceFilter, IFilterStripper> filterToStripper = new LinkedHashMap<>();
		for (ISliceFilter f : distinctFilters) {
			if (!ISliceFilter.MATCH_NONE.equals(f)) {
				filterToStripper.put(f, stripperFactory.makeFilterStripper(f));
			}
		}
		return filterToStripper;
	}

	/**
	 * Builds the filter strictness index: for each distinct inducer filter, the set of induced filters that are
	 * strictly stricter (i.e., the inducer filter is laxer). Uses the pre-built stripper map to avoid additional
	 * {@code makeFilterStripper} calls.
	 *
	 * @param distinctFilters
	 *            all distinct filters present in the context group
	 * @param filterToStripper
	 *            pre-built strippers, one per distinct non-MATCH_NONE filter
	 * @return map from inducer filter to the set of valid (stricter) induced filters
	 */
	protected Map<ISliceFilter, Set<ISliceFilter>> makeInducerFilterToStricterInducedFilters(
			ImmutableSet<ISliceFilter> distinctFilters,
			Map<ISliceFilter, IFilterStripper> filterToStripper) {
		Map<ISliceFilter, Set<ISliceFilter>> inducerFilterToStricterInducedFilters = new LinkedHashMap<>();
		for (ISliceFilter fInduced : distinctFilters) {
			if (ISliceFilter.MATCH_NONE.equals(fInduced)) {
				// MATCH_NONE steps are independent: no inducer can serve them
				continue;
			}
			IFilterStripper inducedStripper = filterToStripper.get(fInduced);
			for (ISliceFilter fInducer : distinctFilters) {
				if (inducedStripper.isStricterThan(fInducer)) {
					// fInducer is laxer than fInduced → fInducer can serve fInduced
					inducerFilterToStricterInducedFilters.computeIfAbsent(fInducer, k -> new LinkedHashSet<>())
							.add(fInduced);
				}
			}
		}
		return inducerFilterToStricterInducedFilters;
	}
}
