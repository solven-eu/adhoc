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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.SplitTableQueries;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
import lombok.extern.slf4j.Slf4j;

/**
 * Given a set of {@link CubeQueryStep}, we determine a minimal set of {@link CubeQueryStep} which can infer (by Adhoc)
 * all the others.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class InduceByAdhoc implements ITableStepsSplitter {

	@Override
	public DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> splitInducedAsDag(IHasQueryOptions hasOptions,
			Set<CubeQueryStep> tableSteps) {
		ListMultimap<CubeQueryStep, CubeQueryStep> aggregatorToQueries = packByAggregator(tableSteps);

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer =
				new DirectedAcyclicGraph<>(DefaultEdge.class);

		// for each aggregator
		aggregatorToQueries.asMap().forEach((a, steps) -> {
			DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> aInducedToInducer =
					new DirectedAcyclicGraph<>(DefaultEdge.class);

			// BEWARE step.filter must be optimized as it is inserted in a hashStructure
			steps.forEach(aInducedToInducer::addVertex);
			splitInducedDag(steps, aInducedToInducer::addEdge);

			if (hasOptions.isDebugOrExplain()) {
				SplitTableQueries aTableQueries =
						SplitTableQueries.builder().inducedToInducer(aInducedToInducer).build();

				// TODO This log lacks options and customMarkers if any
				log.info("[EXPLAIN] inducers={} induceds={} roots={} for agg={}",
						aTableQueries.getInducers().size(),
						aTableQueries.getInduceds().size(),
						aTableQueries.getRoots().size(),
						a.getMeasure().getName());
			}

			// accumulate into the output graph
			Graphs.addGraph(inducedToInducer, aInducedToInducer);
		});

		return inducedToInducer;
	}

	protected CubeQueryStep contextOnly(CubeQueryStep inducer) {
		return inducer.toBuilder()
				.measure("noMeasure")
				.groupBy(IGroupBy.GRAND_TOTAL)
				.filter(ISliceFilter.MATCH_ALL)
				.build();
	}

	protected ListMultimap<CubeQueryStep, CubeQueryStep> packByAggregator(Set<CubeQueryStep> rootInducers) {
		ListMultimap<CubeQueryStep, CubeQueryStep> contextualAggregateToQueries =
				MultimapBuilder.linkedHashKeys().arrayListValues().build();

		rootInducers.forEach(tq -> {
			IMeasure measure = tq.getMeasure();

			if (!(measure instanceof Aggregator agg)) {
				throw new IllegalArgumentException("TableQueryOptimizer require steps to be on Aggregator. Was " + tq);
			}

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
		return contextualAggregateToQueries;
	}

	/**
	 * Given an input set of steps, callback on edge of an inducing DAG. The returned DAG looks for the edge which are
	 * the simplest one (e.g. given `a`, `a,b` and `a,b,c`, while `a` can induce both `a,b` and `a,b,c`, we prefer to
	 * have `a,b` as inducer of `a,b,c`).
	 * 
	 * This minimizing strategy helps minimizing the actual computations when reducing (e.g. it is easier to infer `a`
	 * given `a,b` than given `a,b,c`).
	 * 
	 * TODO it may not be possible/optimal to evaluate before the best inducingEdge. Typically, we may have `a,b,d` with
	 * only one row and `a,b,c` with 3 rows: `a,b,d` is a better candidate to infer `a`).
	 * 
	 * @param steps
	 * @param onInducedToInducer
	 */
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	protected void splitInducedDag(Collection<CubeQueryStep> steps,
			BiConsumer<CubeQueryStep, CubeQueryStep> onInducedToInducer) {
		if (steps.isEmpty()) {
			return;
		}

		// groupBy number of groupedBy columns, in order to filter the candidate tableQueries
		// GroupBy tableQueries by groupBy cardinality, as we're guaranteed that a tableQuery with more groupBy can
		// not be inferred by a tableQUery with less groupBys.
		Map<Integer, List<CubeQueryStep>> cardinalityToSteps = steps.stream()
				.collect(Collectors.groupingBy(s -> s.getGroupBy().getGroupedByColumns().size(),
						LinkedHashMap::new,
						Collectors.toList()));

		int maxGroupBy = cardinalityToSteps.keySet().stream().mapToInt(i -> i).max().getAsInt();

		// BEWARE Following algorithm is quadratic: for each tableQuery, we evaluate all other tableQuery.
		// We observed up to 1k steps.
		steps.forEach(induced -> {
			// inducer must have more groupBys than induced
			int smallestGroupBy = induced.getGroupBy().getGroupedByColumns().size();
			for (int inducerGroupBy = smallestGroupBy; inducerGroupBy <= maxGroupBy; inducerGroupBy++) {
				Optional<CubeQueryStep> optInducer = cardinalityToSteps.get(inducerGroupBy)
						.stream()
						// No edge to itself
						.filter(inducer -> inducer != induced)
						// Compatible context (i.e. laxer filter, customMarker, options)
						.filter(inducer -> canInduce(inducer, induced))
						// as soon as left is induced, we do not need to search for alternative inducer
						// BEWARE As we find first, we'll spot the inducer with a minimal additional groupBy, hence
						// probably a smaller induced, hence probably a better inducer
						.findFirst();

				if (optInducer.isPresent()) {
					CubeQueryStep inducer = optInducer.get();
					// right can be used to compute left
					onInducedToInducer.accept(induced, inducer);
					log.trace("Induced -> Inducer ({} -> {})", induced, inducer);

					break;
				} else {
					log.trace("Scan along candidates with more groupBys");
				}
			}

			log.trace("No inducer (as it is a root inducer) for {}", induced);
		});
	}

	// Typically: `groupBy:ccy+country;ccy=EUR|USD` can induce `ccy=EUR`
	// BEWARE This design prevents having an induced inferred by multiple inducers
	// (e.g. `WHERE A` and `WHERE B` can induce `WHERE A OR B`)
	protected boolean canInduce(CubeQueryStep inducer, CubeQueryStep induced) {
		if (!inducer.getMeasure().getName().equals(induced.getMeasure().getName())) {
			// Different measures: can not induce
			return false;
		} else if (!contextOnly(inducer).equals(contextOnly(induced))) {
			// Different options/customMarker: can not induce
			return false;
		}

		// BEWARE a given name may refer to a ReferencedColumn, or to a StaticCoordinateColumn (or anything else)
		Collection<IAdhocColumn> inducerColumns = inducer.getGroupBy().getNameToColumn().values();
		Collection<IAdhocColumn> inducedColumns = induced.getGroupBy().getNameToColumn().values();
		if (!inducerColumns.containsAll(inducedColumns)) {
			// Not expressing all needed columns: can not induce
			// If right has all groupBy of left, it means right has same or more groupBy than left,
			// hence right can be used to compute left
			return false;
		}

		ISliceFilter inducerFilter = inducer.getFilter();
		ISliceFilter inducedFilter = induced.getFilter();

		if (!FilterHelpers.isStricterThan(inducedFilter, inducerFilter)) {
			// Induced is not covered by inducer: it can not infer it
			return false;
		}

		Optional<ISliceFilter> leftoverFilter =
				InducerHelpers.makeLeftoverFilter(inducerColumns, inducerFilter, inducedFilter);

		if (leftoverFilter.isEmpty()) {
			// Inducer is missing columns to reject rows not expected by induced
			return false;
		}

		return true;
	}

}
