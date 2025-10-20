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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.FilterUtility;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.query.filter.optimizer.IHasFilterStripperFactory;
import eu.solven.adhoc.query.filter.stripper.FilterStripper;
import eu.solven.adhoc.query.filter.stripper.IFilterStripper;
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
	protected DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> splitInducedAsDag(Set<TableQuery> tableQueries) {
		// This dag optimized `induced->inducer` by minimizing the number of inducers, considering only inducers from
		// the initial TableQueries
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer = super.splitInducedAsDag(tableQueries);

		Set<CubeQueryStep> rootInducers =
				SplitTableQueries.builder().inducedToInducer(inducedToInducer).build().getInducers();

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> moreInducedToInducer = getGroupedInducers(rootInducers);

		moreInducedToInducer.vertexSet().forEach(inducedToInducer::addVertex);
		moreInducedToInducer.edgeSet()
				.forEach(e -> inducedToInducer.addEdge(moreInducedToInducer.getEdgeSource(e),
						moreInducedToInducer.getEdgeTarget(e)));

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
		ListMultimap<CubeQueryStep, CubeQueryStep> contextualAggregateToQueries =
				MultimapBuilder.linkedHashKeys().arrayListValues().build();

		rootInducers.forEach(tq -> {
			IMeasure agg = tq.getMeasure();
			// Typically holds options and customMarkers
			CubeQueryStep contextOnly = contextOnly(CubeQueryStep.edit(tq).measure(agg).build());

			// consider a single context per measure
			CubeQueryStep singleAggregator = CubeQueryStep.edit(contextOnly).measure(agg).build();

			CubeQueryStep aggregatorStep = CubeQueryStep.edit(contextOnly)
					.measure(agg)
					.groupBy(tq.getGroupBy())
					.filter(tq.getFilter())
					.build();

			contextualAggregateToQueries.put(singleAggregator, aggregatorStep);
		});

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

			Set<String> inducerColumns = new TreeSet<>();
			Set<ISliceFilter> eachInducedFilters = new LinkedHashSet<>();
			filterGroupBy.forEach(tq -> {
				inducerColumns.addAll(tq.getGroupBy().getGroupedByColumns());

				ISliceFilter strippedFromWhere = commonStripper.strip(tq.getFilter());
				// We need these additional columns for proper filtering
				inducerColumns.addAll(FilterHelpers.getFilteredColumns(strippedFromWhere));

				eachInducedFilters.add(strippedFromWhere);
			});

			// OR between each inducer own filter
			// induced will fetch the union of rows for all induced
			ISliceFilter combinedOr = FilterBuilder.or(eachInducedFilters).optimize(filterOptimizer);
			// BEWARE This `AND` is optimized even if we expect no optimization.
			// Even if we would expect low amount of optimizations, it require optimization in a later step
			ISliceFilter inducerFilter = FilterBuilder.and(commonFilter, combinedOr).optimize(filterOptimizer);

			CubeQueryStep inducer = CubeQueryStep.edit(contextualAggregate)
					.filter(inducerFilter)
					.groupBy(GroupByColumns.named(inducerColumns))
					.build();

			filterGroupBy.forEach(tq -> {
				ISliceFilter inducedFilter = FilterBuilder.and(inducerFilter, tq.getFilter()).optimize(filterOptimizer);
				CubeQueryStep induced = CubeQueryStep.edit(tq).filter(inducedFilter).build();

				if (inducer.equals(induced)) {
					// e.g. `GROUP BY a,b` and `GROUP BY a`
					log.trace("Happens typically if we query a granular and an induced groupBy with same filter");
				} else {
					inducedToInducer.addVertex(inducer);
					inducedToInducer.addVertex(induced);
					inducedToInducer.addEdge(induced, inducer);
				}
			});
		});

		return inducedToInducer;
	}

	protected IFilterStripper makeShareFilterStripper() {
		if (filterOptimizer instanceof IHasFilterStripperFactory hasFilterStripperFactory) {
			return hasFilterStripperFactory.getFilterStripperFactory().makeFilterStripper(ISliceFilter.MATCH_ALL);
		} else {
			return AdhocUnsafe.filterStripperFactory.makeFilterStripper(ISliceFilter.MATCH_ALL);
		}
	}

}
