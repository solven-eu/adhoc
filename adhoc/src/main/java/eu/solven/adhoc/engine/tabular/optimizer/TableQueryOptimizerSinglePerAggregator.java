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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.FilterUtility;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.query.filter.optimizer.IHasFilterStripperFactory;
import eu.solven.adhoc.query.filter.stripper.IFilterStripper;
import eu.solven.adhoc.query.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.extern.slf4j.Slf4j;

/**
 * The main strategy of this {@link ITableQueryOptimizer} is to evaluate the minimal number of {@link TableQuery} needed
 * to compute all {@link TableQuery}, allowing to compute irrelevant aggregates. Typically, it will evaluate the union
 * of {@link IAdhocGroupBy} and an {@link OrFilter} amongst all {@link ISliceFilter}.
 * 
 * In short, it enables doing a single query per measure to the {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class TableQueryOptimizerSinglePerAggregator extends TableQueryOptimizer {

	// Rely on an filterOptimizer with cache as this tableQueryOptimizer may collect a large number of filters into
	// a single query, leading to a very large OR.
	public TableQueryOptimizerSinglePerAggregator(AdhocFactories factories, IFilterOptimizer filterOptimizer) {
		super(factories, filterOptimizer);
	}

	@Override
	protected DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> splitInducedAsDag(IHasQueryOptions hasOptions,
			Set<CubeQueryStep> tableQueries) {
		// This dag optimized `induced->inducer` by minimizing the number of inducers, considering only inducers from
		// the initial TableQueries
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer =
				super.splitInducedAsDag(hasOptions, tableQueries);

		// rootInducers can imply all other steps
		Set<CubeQueryStep> rootInducers =
				SplitTableQueries.builder().inducedToInducer(inducedToInducer).build().getInducers();

		// Make an alternative graph, computing rootInducers from another set of steps
		// e.g. groupBy aggregators, and union the GROUP_BY and FILTER clauses.
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> moreInducedToInducer = getGroupedInducers(rootInducers);

		if (hasOptions.isDebugOrExplain()) {
			SplitTableQueries preSingle = SplitTableQueries.builder().inducedToInducer(inducedToInducer).build();
			SplitTableQueries postSingle = SplitTableQueries.builder().inducedToInducer(moreInducedToInducer).build();
			log.info("[EXPLAIN] singleAggregator from inducers={} to inducers={}",
					preSingle.getInducers().size(),
					postSingle.getInducers().size());
		}

		// Extend the DAG with this new DAG
		Graphs.addGraph(inducedToInducer, moreInducedToInducer);

		return inducedToInducer;
	}

	/**
	 * The splitting strategy is based on:
	 * <ul>
	 * <li>Doing a single query per Aggregator. It will keep a low number of queries</li>
	 * <li>For an aggregator, do a query able to induce all steps (typically by querying the union of groupBy and
	 * filters)</li>
	 * </ul>
	 * 
	 * From an implementation perspective, this re-use the standard optimization process, then compute a single
	 * CubeQueryStep by Aggregator given the root inducers.
	 */
	protected DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> getGroupedInducers(Set<CubeQueryStep> rootInducers) {
		ListMultimap<CubeQueryStep, CubeQueryStep> contextualAggregateToQueries = packByAggregator(rootInducers);

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer =
				new DirectedAcyclicGraph<>(DefaultEdge.class);

		IFilterStripper sharedStripper = makeShareFilterStripper();

		FilterUtility filterUtility = FilterUtility.builder().optimizer(filterOptimizer).build();
		contextualAggregateToQueries.asMap().forEach((contextualAggregate, filterGroupBy) -> {
			Set<? extends ISliceFilter> filters =
					filterGroupBy.stream().map(CubeQueryStep::getFilter).collect(ImmutableSet.toImmutableSet());

			ISliceFilter rawCommonAnd = filterUtility.commonAnd(filters);
			ISliceFilter commonFilter = FilterBuilder.and(rawCommonAnd).optimize(filterOptimizer);

			IFilterStripper commonStripper = sharedStripper.withWhere(commonFilter);

			// Do the UNION of `GROUP BY` and `FILTER`
			Set<String> inducerColumns = new TreeSet<>();
			Set<ISliceFilter> eachInducedFilters = new LinkedHashSet<>();
			ListMultimap<ISliceFilter, CubeQueryStep> strippedFilterToStep =
					MultimapBuilder.linkedHashKeys().arrayListValues().build();
			filterGroupBy.forEach(tq -> {
				inducerColumns.addAll(tq.getGroupBy().getGroupedByColumns());

				ISliceFilter strippedFromWhere = commonStripper.strip(tq.getFilter());
				// We need these additional columns for proper filtering
				inducerColumns.addAll(FilterHelpers.getFilteredColumns(strippedFromWhere));

				strippedFilterToStep.put(strippedFromWhere, tq);

				eachInducedFilters.add(strippedFromWhere);
			});

			// OR between each inducer own filter induced will fetch the union of rows for all induced
			ISliceFilter combinedOr = FilterBuilder.or(eachInducedFilters).optimize(filterOptimizer);
			// BEWARE This `AND` is optimized even if we expect no optimization.
			// Even if we would expect low amount of optimizations, it require optimization in a later step
			ISliceFilter inducerFilter = FilterBuilder.and(commonFilter, combinedOr).optimize(filterOptimizer);

			CubeQueryStep inducer = CubeQueryStep.edit(contextualAggregate)
					.filter(inducerFilter)
					.groupBy(GroupByColumns.named(inducerColumns))
					.build();

			strippedFilterToStep.asMap().forEach((strippedFilter, steps) -> {
				if (steps.isEmpty()) {
					// Should not happen by construction
					log.warn("Empty steps?");
				} else if (ISliceFilter.MATCH_ALL.equals(strippedFilter) || steps.size() == 1) {
					steps.forEach(induced -> {
						addInducerToInduced(inducedToInducer, inducer, induced);
					});
				} else {
					// There is 2 steps with the same strippedFilter: let's add an intermediate step applying given
					// filter
					CubeQueryStep intermediate = makeIntermediateStep(commonFilter, inducer, strippedFilter, steps);

					addInducerToInduced(inducedToInducer, inducer, intermediate);
					steps.forEach(induced -> {
						addInducerToInduced(inducedToInducer, intermediate, induced);
					});
				}
			});

		});

		return inducedToInducer;
	}

	/**
	 * 
	 * @param commonFilter
	 * @param inducer
	 * @param strippedFilter
	 * @param steps
	 * @return a {@link CubeQueryStep} which can be used as intermediate to the input steps.
	 */
	protected CubeQueryStep makeIntermediateStep(ISliceFilter commonFilter,
			CubeQueryStep inducer,
			ISliceFilter strippedFilter,
			Collection<CubeQueryStep> steps) {
		ISliceFilter intermediateFilter = FilterBuilder.and(commonFilter, strippedFilter).optimize(filterOptimizer);

		// Do the UNION of `GROUP BY` and `FILTER`
		Set<String> intermediateColumns = new TreeSet<>();
		steps.forEach(tq -> {
			intermediateColumns.addAll(tq.getGroupBy().getGroupedByColumns());
			// We need these additional columns for proper filtering
			intermediateColumns.addAll(FilterHelpers.getFilteredColumns(strippedFilter));
		});

		return CubeQueryStep.edit(inducer)
				.filter(intermediateFilter)
				.groupBy(GroupByColumns.named(intermediateColumns))
				.build();
	}

	protected void addInducerToInduced(DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer,
			CubeQueryStep inducer,
			CubeQueryStep induced) {
		if (inducer.equals(induced)) {
			// e.g. `GROUP BY a,b WHERE b` and `GROUP BY a WHERE b`
			log.trace("Happens typically if we query a granular and an induced groupBy with same filter");
		} else {
			inducedToInducer.addVertex(inducer);
			inducedToInducer.addVertex(induced);
			inducedToInducer.addEdge(induced, inducer);
		}
	}

	protected IFilterStripper makeShareFilterStripper() {
		IFilterStripperFactory filterStripperFactory;
		if (filterOptimizer instanceof IHasFilterStripperFactory hasFilterStripperFactory) {
			filterStripperFactory = hasFilterStripperFactory.getFilterStripperFactory();
		} else {
			filterStripperFactory = AdhocUnsafe.filterStripperFactory;
		}
		return filterStripperFactory.makeFilterStripper(ISliceFilter.MATCH_ALL);
	}

}
