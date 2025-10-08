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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.query.table.TableQuery;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard {@link ITableQueryOptimizer}. It works on a per-Aggregator basis, evaluating which one can be skipped given
 * a minimal number of {@link CubeQueryStep}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class TableQueryOptimizer extends ATableQueryOptimizer {

	public TableQueryOptimizer(AdhocFactories factories, IFilterOptimizer filterOptimizer) {
		super(factories, filterOptimizer);
	}

	/**
	 * 
	 * @param tableQueries
	 * @return an Object partitioning TableQuery which can not be induced from those which can be induced.
	 */
	@Override
	public SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<TableQuery> tableQueries) {
		if (tableQueries.isEmpty()) {
			return SplitTableQueries.empty();
		}

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer = splitInducedAsDag(tableQueries);

		return SplitTableQueries.builder().inducedToInducer(inducedToInducer).build();
	}

	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	protected DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> splitInducedAsDag(Set<TableQuery> tableQueries) {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer =
				new DirectedAcyclicGraph<>(DefaultEdge.class);

		// Inference in aggregator based: `k1` does not imply `k2`, `k1.SUM` does not imply `k1.MAX`
		// TODO The key should includes options and customMarker
		SetMultimap<Aggregator, CubeQueryStep> aggregatorToQueries =
				MultimapBuilder.hashKeys().linkedHashSetValues().build();

		// Register all tableQueries as a vertex
		tableQueries.forEach(tq -> {
			tq.getAggregators().stream().forEach(agg -> {
				CubeQueryStep step = CubeQueryStep.edit(tq).measure(agg).build();
				// BEWARE step.filter must be optimized as it is inserted in a hashStructure
				inducedToInducer.addVertex(step);
				aggregatorToQueries.put(agg, step);
			});
		});

		// BEWARE Following algorithm is quadratic: for each tableQuery, we evaluate all other tableQuery.
		aggregatorToQueries.asMap().forEach((a, steps) -> {
			// groupBy number of groupedBy columns, in order to filter the candidate tableQueries
			int maxGroupBy =
					steps.stream().mapToInt(tb -> tb.getGroupBy().getGroupedByColumns().size()).max().getAsInt();
			List<Set<CubeQueryStep>> nbGroupByToQueries = IntStream.rangeClosed(0, maxGroupBy)
					.<Set<CubeQueryStep>>mapToObj(i -> new LinkedHashSet<>())
					.toList();

			// GroupBy tableQueries by groupBy cardinality, as we're guaranteed that a tableQuery with more groupBy can
			// not be inferred by a tableQUery with less groupBys.
			steps.forEach(step -> {
				nbGroupByToQueries.get(step.getGroupBy().getGroupedByColumns().size()).add(step);
			});

			steps.forEach(induced -> {
				AtomicBoolean hasFoundInducer = new AtomicBoolean();

				// right must have more groupBys than left, else right can not induce left
				for (int inducerGroupByCardinality = induced.getGroupBy()
						.getGroupedByColumns()
						.size(); inducerGroupByCardinality < nbGroupByToQueries.size(); inducerGroupByCardinality++) {
					nbGroupByToQueries.get(inducerGroupByCardinality)
							.stream()
							// No edge to itself
							.filter(inducer -> inducer != induced)
							// Same context (i.e. same filter, customMarker, options)
							.filter(inducer -> canInduce(inducer, induced))
							// as soon as left is induced, we do not need to search for alternative inducer
							// BEWARE As we find first, we'll spot the inducer with a minimal additional groupBy, hence
							// probably a smaller induced, hence probably a better inducer
							.findFirst()
							.ifPresent(inducer -> {
								// right can be used to compute left
								inducedToInducer.addEdge(induced, inducer);
								hasFoundInducer.set(true);
							});

					if (hasFoundInducer.get()) {
						break;
					}
				}
			});
		});
		return inducedToInducer;
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

		Optional<ISliceFilter> leftoverFilter = makeLeftoverFilter(inducerColumns, inducerFilter, inducedFilter);

		if (leftoverFilter.isEmpty()) {
			// Inducer is missing columns to reject rows not expected by induced
			return false;
		}

		return true;
	}

}
