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

import java.util.Set;
import java.util.TreeSet;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer.SplitTableQueries.SplitTableQueriesBuilder;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * The main strategy of this {@link ITableQueryOptimizer} is to evaluate the minimal number of {@link TableQuery} needed
 * to compute all {@link TableQuery}, allowing to compute irrelevant aggregates. Typically, it will evaluate the union
 * of {@link IAdhocGroupBy} and an {@link OrFilter} amongst all {@link ISliceFilter}.
 * 
 * In short, it enables doing a single query per measure to the {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 */
public class TableQueryOptimizerSinglePerAggregator extends ATableQueryOptimizer {

	public TableQueryOptimizerSinglePerAggregator(AdhocFactories factories) {
		super(factories);
	}

	@Override
	public SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<TableQuery> tableQueries) {
		if (tableQueries.isEmpty()) {
			return SplitTableQueries.empty();
		}

		ListMultimap<CubeQueryStep, CubeQueryStep> contextualAggregateToQueries =
				MultimapBuilder.linkedHashKeys().arrayListValues().build();

		tableQueries.forEach(tq -> {
			tq.getAggregators().forEach(agg -> {
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
		});

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

		SplitTableQueriesBuilder split = SplitTableQueries.builder();
		contextualAggregateToQueries.asMap().forEach((contextualAggregate, filterGroupBy) -> {
			Set<String> inducerColumns = new TreeSet<>();

			Set<? extends ISliceFilter> filters =
					filterGroupBy.stream().map(CubeQueryStep::getFilter).collect(ImmutableSet.toImmutableSet());
			ISliceFilter commonFilter = FilterHelpers.commonFilter(filters);

			filterGroupBy.forEach(tq -> {
				inducerColumns.addAll(tq.getGroupBy().getGroupedByColumns());

				ISliceFilter strippedFromWhere = FilterHelpers.stripWhereFromFilter(commonFilter, tq.getFilter());
				// We need these additional columns for proper filtering
				inducerColumns.addAll(FilterHelpers.getFilteredColumns(strippedFromWhere));
			});

			// OR between each inducer own filter
			Set<ISliceFilter> eachInducedFilters = filterGroupBy.stream()
					.map(tq -> FilterHelpers.stripWhereFromFilter(commonFilter, tq.getFilter()))
					.collect(ImmutableSet.toImmutableSet());
			// induced will fetch the union of rows for all induced
			ISliceFilter inducerFilter = AndFilter.and(commonFilter, OrFilter.or(eachInducedFilters));

			filterGroupBy.forEach(tq -> {
				CubeQueryStep inducerStep = CubeQueryStep.edit(tq)
						.filter(inducerFilter)
						.groupBy(GroupByColumns.named(inducerColumns))
						.build();
				split.inducer(inducerStep);

				ISliceFilter strippedFromWhere = FilterHelpers.stripWhereFromFilter(commonFilter, tq.getFilter());
				CubeQueryStep inducedStep = CubeQueryStep.edit(tq).filter(strippedFromWhere).build();
				split.induced(inducedStep);

				dag.addVertex(inducerStep);
				dag.addVertex(inducedStep);
				dag.addEdge(inducedStep, inducerStep);
			});
		});

		return split.dagToDependancies(dag).build();
	}

}
