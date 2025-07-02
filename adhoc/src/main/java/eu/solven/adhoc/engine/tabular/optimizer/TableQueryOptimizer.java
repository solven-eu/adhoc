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
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
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

	/**
	 * 
	 * @param tableQueries
	 * @return an Object partitioning TableQuery which can not be induced from those which can be induced.
	 */
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	@Override
	public SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<TableQuery> tableQueries) {
		if (tableQueries.isEmpty()) {
			return SplitTableQueries.empty();
		}

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dagToDependancies =
				new DirectedAcyclicGraph<>(DefaultEdge.class);

		// Inference in aggregator based: `k1` does not imply `k2`, `k1.SUM` does not imply `k1.MAX`
		// TODO The key should includes options and customMarker
		SetMultimap<Aggregator, CubeQueryStep> aggregatorToQueries =
				MultimapBuilder.hashKeys().linkedHashSetValues().build();

		// Register all tableQueries as a vertex
		tableQueries.forEach(tq -> {
			tq.getAggregators().stream().forEach(agg -> {
				CubeQueryStep step = CubeQueryStep.edit(tq).measure(agg).build();
				dagToDependancies.addVertex(step);
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
				for (int i = induced.getGroupBy().getGroupedByColumns().size(); i < nbGroupByToQueries.size(); i++) {
					nbGroupByToQueries.get(i)
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
								dagToDependancies.addEdge(induced, inducer);
								hasFoundInducer.set(true);

								if (hasOptions.isDebugOrExplain()) {
									log.info("[EXPLAIN] {} will induce {}", inducer, induced);
								}
							});

					if (hasFoundInducer.get()) {
						break;
					}
				}
			});
		});

		// Collect the tableQueries which can not be induced by another tableQuery
		Set<CubeQueryStep> notInduced = dagToDependancies.vertexSet()
				.stream()
				.filter(tq -> dagToDependancies.outgoingEdgesOf(tq).isEmpty())
				.collect(ImmutableSet.toImmutableSet());
		// Collect the tableQueries which can be induced
		Set<CubeQueryStep> induced = ImmutableSet.copyOf(Sets.difference(dagToDependancies.vertexSet(), notInduced));

		return SplitTableQueries.builder()
				.inducers(notInduced)
				.induceds(induced)
				.dagToDependancies(dagToDependancies)
				.build();
	}

	// Typically: `groupBy:ccy&ccy=EUR|USD` can induce `ccy=EUR`
	protected boolean canInduce(CubeQueryStep inducer, CubeQueryStep induced) {
		if (!inducer.getMeasure().getName().equals(induced.getMeasure().getName())) {
			// Different measures: can not induce
			return false;
		} else if (!contextOnly(inducer).equals(contextOnly(induced))) {
			// Different options/customMarker: can not induce
			return false;
		}

		NavigableSet<String> inducerColumns = inducer.getGroupBy().getGroupedByColumns();
		NavigableSet<String> inducedColumns = induced.getGroupBy().getGroupedByColumns();
		if (!inducerColumns.containsAll(inducedColumns)) {
			// Not expressing all needed columns: can not induce
			// If right has all groupBy of left, it means right has same or more groupBy than left,
			// hence right can be used to compute left
			return false;
		}

		IAdhocFilter inducerFilter = inducer.getFilter();
		IAdhocFilter inducedFilter = induced.getFilter();

		if (!AndFilter.and(inducerFilter, inducedFilter).equals(inducedFilter)) {
			// Inducer is stricter than induced: it can not infer it
			return false;
		}

		if (inducerColumns.containsAll(FilterHelpers.getFilteredColumns(inducedFilter))) {
			// Inducer has enough columns to apply induced filter
			return true;
		}

		// We may lack column
		if (inducerFilter.equals(inducedFilter)) {
			return true;
		}

		// TODO This comparison should be done only on the filter in induced not present in inducer
		return false;
	}

}
