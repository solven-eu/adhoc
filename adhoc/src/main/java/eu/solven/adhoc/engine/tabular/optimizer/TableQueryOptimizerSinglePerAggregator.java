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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;

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
		ListMultimap<CubeQueryStep, CubeQueryStep> contextualAggregateToSteps = packByAggregator(rootInducers);

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer =
				new DirectedAcyclicGraph<>(DefaultEdge.class);

		IFilterStripper stripper = makeFilterStripper();

		FilterUtility filterUtility = FilterUtility.builder().optimizer(filterOptimizer).build();
		contextualAggregateToSteps.asMap().forEach((contextualAggregate, filterGroupBy) -> {
			DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> aInducedToInducer =
					makeAggregatorDag(stripper, filterUtility, contextualAggregate, filterGroupBy);

			Graphs.addGraph(inducedToInducer, aInducedToInducer);

		});

		return inducedToInducer;
	}

	protected DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> makeAggregatorDag(IFilterStripper stripper,
			FilterUtility filterUtility,
			CubeQueryStep contextualAggregate,
			Collection<CubeQueryStep> filterGroupBy) {
		// `a&b|a&b|a&c|d` should lead to `a&(b|c)|d` to the tableQuery
		// and `a&b` as intermediate step, useful to prepare both `a&b` steps
		// and `a` as intermediate step, useful to prepare `a&b` and `a&c`
		AtomicLongMap<ISliceFilter> filterPartToCount = AtomicLongMap.create();
		SetMultimap<CubeQueryStep, ISliceFilter> filterToParts =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

		filterGroupBy.forEach(step -> {
			Set<ISliceFilter> split = FilterHelpers.splitAnd(step.getFilter());
			split.forEach(filterPartToCount::incrementAndGet);
			filterToParts.putAll(step, split);
		});

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer =
				new DirectedAcyclicGraph<>(DefaultEdge.class);
		Optional<Map.Entry<ISliceFilter, Long>> optMostPresentPart = filterPartToCount.asMap()
				.entrySet()
				.stream()
				// Consider only parts which are present in at least 2 steps
				.filter(e -> e.getValue() >= 2)
				// TODO Ordering as AtomicLongMap is not Linked, and we want deterministic behavior
				.max(Map.Entry.<ISliceFilter, Long>comparingByValue()
						.<ISliceFilter>thenComparing(Map.Entry::getKey, FilterHelpers.filterComparator()));

		if (optMostPresentPart.isPresent()) {
			Map.Entry<ISliceFilter, Long> mostPresentPart = optMostPresentPart.get();
			ImmutableSet<CubeQueryStep> stepsWithMostPresentPart = filterToParts.asMap()
					.entrySet()
					.stream()
					.filter(e -> e.getValue().contains(mostPresentPart.getKey()))
					.map(Map.Entry::getKey)
					.collect(ImmutableSet.toImmutableSet());

			ImmutableSet<CubeQueryStep> otherSteps;
			if (stepsWithMostPresentPart.size() == filterGroupBy.size()) {
				otherSteps = ImmutableSet.of();
			} else {
				otherSteps = ImmutableSet
						.copyOf(Sets.difference(ImmutableSet.copyOf(filterGroupBy), stepsWithMostPresentPart));
			}

			Set<? extends ISliceFilter> filters = stepsWithMostPresentPart.stream()
					.map(CubeQueryStep::getFilter)
					.collect(ImmutableSet.toImmutableSet());

			// Evaluate the `WHERE` common to all steps of given aggregate
			ISliceFilter rawCommonFilter = filterUtility.commonAnd(filters);
			ISliceFilter commonFilter = FilterBuilder.and(rawCommonFilter).optimize(filterOptimizer);

			IFilterStripper commonStripper = stripper.withWhere(commonFilter);

			List<CubeQueryStep> strippedWithCommon = stepsWithMostPresentPart.stream().map(step -> {
				ISliceFilter strippedFromWhere = commonStripper.strip(step.getFilter());
				return CubeQueryStep.edit(step).filter(strippedFromWhere).build();
			}).toList();

			// `a&b|a&b|a&c`
			DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> subDagCommon =
					makeAggregatorDag(stripper, filterUtility, contextualAggregate, strippedWithCommon);
			subDagCommon.vertexSet().forEach(step -> {
				CubeQueryStep reforgedStep = filter(commonFilter, step);
				inducedToInducer.addVertex(reforgedStep);
			});
			subDagCommon.edgeSet().forEach(edge -> {
				inducedToInducer.addEdge(filter(commonFilter, inducedToInducer.getEdgeSource(edge)),
						filter(commonFilter, inducedToInducer.getEdgeTarget(edge)));
			});

			// `d`
			DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> subDagOther =
					makeAggregatorDag(stripper, filterUtility, contextualAggregate, otherSteps);
			Graphs.addGraph(inducedToInducer, subDagOther);
		} else {
			// nothing to share
			filterGroupBy.forEach(induced -> {
				inducedToInducer.addVertex(induced);
			});
		}

		List<CubeQueryStep> inducers =
				inducedToInducer.vertexSet().stream().filter(s -> inducedToInducer.outDegreeOf(s) == 0).toList();

		// Do the UNION of `GROUP BY` and `FILTER`
		Set<String> inducerColumns = new TreeSet<>();
		Set<ISliceFilter> eachInducedFilters = new LinkedHashSet<>();

		// This relates to the steps executed by the table
		// In this case, we need to add a filtered column if it is not common to all steps, as it will be needed to
		// filter rows for given sub-steps
		ISliceFilter rawCommonFilter =
				filterUtility.commonAnd(inducers.stream().map(CubeQueryStep::getFilter).toList());
		ISliceFilter commonFilter = FilterBuilder.and(rawCommonFilter).optimize(filterOptimizer);
		IFilterStripper commonStripper = stripper.withWhere(commonFilter);

		inducers.forEach(step -> {
			// UNION of the `GROUP BY`
			inducerColumns.addAll(step.getGroupBy().getGroupedByColumns());

			// We need these additional columns for proper filtering
			ISliceFilter stripped = commonStripper.strip(step.getFilter());
			inducerColumns.addAll(FilterHelpers.getFilteredColumns(stripped));

			eachInducedFilters.add(step.getFilter());
		});

		// OR between each inducer own filter induced will fetch the union of rows for all induced
		ISliceFilter combinedOr = FilterBuilder.or(eachInducedFilters).optimize(filterOptimizer);

		CubeQueryStep inducer = CubeQueryStep.edit(contextualAggregate)
				.filter(combinedOr)
				.groupBy(GroupByColumns.named(inducerColumns))
				.build();

		inducers.forEach(induced -> {
			addInducerToInduced(inducedToInducer, inducer, induced);
		});

		return inducedToInducer;
	}

	protected CubeQueryStep filter(ISliceFilter commonFilter, CubeQueryStep step) {
		ISliceFilter combinedFilter = FilterBuilder.and(commonFilter, step.getFilter()).optimize(filterOptimizer);
		return CubeQueryStep.edit(step).filter(combinedFilter).build();
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

	protected IFilterStripper makeFilterStripper() {
		IFilterStripperFactory filterStripperFactory;
		if (filterOptimizer instanceof IHasFilterStripperFactory hasFilterStripperFactory) {
			filterStripperFactory = hasFilterStripperFactory.getFilterStripperFactory();
		} else {
			filterStripperFactory = AdhocUnsafe.filterStripperFactory;
		}
		return filterStripperFactory.makeFilterStripper(ISliceFilter.MATCH_ALL);
	}

}
