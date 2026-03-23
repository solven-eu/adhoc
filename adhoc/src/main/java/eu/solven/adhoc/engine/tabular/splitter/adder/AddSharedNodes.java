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
package eu.solven.adhoc.engine.tabular.splitter.adder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphHelpers;
import eu.solven.adhoc.engine.tabular.splitter.InduceByAdhoc;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.FilterUtility;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.filter.stripper.IFilterStripper;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A policy to add shared nodes, to reduce the workload of {@link InduceByAdhoc}.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class AddSharedNodes implements IAddSharedNodes {
	@Default
	final IFilterOptimizer filterOptimizer = AdhocFactoriesUnsafe.factories.getFilterOptimizerFactory().makeOptimizer();

	@Default
	@NonNull
	protected final IFilterStripperFactory filterStripperFactory =
			AdhocFactoriesUnsafe.factories.getFilterStripperFactory();

	public static IAddSharedNodesFactory makeFactory() {
		return filterOptimizer -> AddSharedNodes.builder().filterOptimizer(filterOptimizer).build();
	}

	/**
	 * Given a {@link Graph}, we add some intermediate nodes to help doing once some shared computations. Typically, if
	 * 2 nodes depends on the same inducer, and apply similar {@link ISliceFilter}, we may add an intermediate node
	 * applying the common {@link ISliceFilter}.
	 * 
	 * @param input
	 * @return
	 */
	@Override
	public DirectedAcyclicGraph<TableQueryStep, DefaultEdge> addSharedNodes(
			DirectedAcyclicGraph<TableQueryStep, DefaultEdge> input) {
		DirectedAcyclicGraph<TableQueryStep, DefaultEdge> before = input;

		// One may may need to process multiple times. Indeed, given an inducer, inducing 5 nodes, a
		// first pass may cover 2 nodes, and we may need another pass to process 2 other nodes.
		while (true) {
			DirectedAcyclicGraph<TableQueryStep, DefaultEdge> after = GraphHelpers.copy(before);

			// Iterate along before as after will be modified in-place
			before.vertexSet().stream().forEach(inducer -> {
				addSharedNode(after, inducer);
			});

			if (before.equals(after)) {
				// No more shared nodes
				return after;
			}

			before = after;
		}
	}

	protected void addSharedNode(DirectedAcyclicGraph<TableQueryStep, DefaultEdge> withFinalInducers,
			TableQueryStep inducer) {
		ImmutableSet<TableQueryStep> inducedSteps = GraphHelpers.getInduced(withFinalInducers, inducer);

		if (inducedSteps.size() < 2) {
			return;
		}

		// `a&b|a&b|a&c|d` should lead to `a&(b|c)|d` to the tableQuery
		// and `a&(b|c)` as intermediate step, useful to prepare both `a&b` and `a&c` steps
		// and `a` as intermediate step, useful to prepare `a&b` and `a&c`
		// BEWARE AtomicLongMap is not ordered by we later orderBY some comparator
		AtomicLongMap<ISliceFilter> filterPartToCount = AtomicLongMap.create();
		SetMultimap<TableQueryStep, ISliceFilter> filterToAnds =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

		// This will count `a` 3 times and `b` 2 times
		inducedSteps.forEach(step -> {
			Set<ISliceFilter> split = FilterHelpers.splitAnd(step.getFilter());
			split.forEach(filterPartToCount::incrementAndGet);
			filterToAnds.putAll(step, split);
		});

		List<Entry<ISliceFilter, Long>> orderedMostPresentPart = filterPartToCount.asMap()
				.entrySet()
				.stream()
				// Consider only parts which are present in at least 2 steps
				.filter(e -> e.getValue() >= 2)
				// TODO Ordering as AtomicLongMap is not Linked, and we want deterministic behavior
				.sorted(Map.Entry.<ISliceFilter, Long>comparingByValue()
						.<ISliceFilter>thenComparing(Map.Entry::getKey, FilterHelpers.filterComparator())
						// From most to less common parts
						.reversed())
				.toList();

		// Iterate along the most present parts, as typically, the first part may be very common, and lead to generating
		// as shared node the current inducer
		for (Map.Entry<ISliceFilter, Long> mostPresentPart : orderedMostPresentPart) {
			// One ISliceFilter would be helping at least 2 (possibly all) steps
			ImmutableSet<TableQueryStep> relatedSteps = filterToAnds.asMap()
					.entrySet()
					.stream()
					.filter(e -> e.getValue().contains(mostPresentPart.getKey()))
					.map(Map.Entry::getKey)
					.collect(ImmutableSet.toImmutableSet());

			FilterUtility filterUtility = FilterUtility.builder().optimizer(filterOptimizer).build();

			Set<? extends ISliceFilter> filters =
					relatedSteps.stream().map(TableQueryStep::getFilter).collect(ImmutableSet.toImmutableSet());

			// Evaluate the `WHERE` common to all steps of given aggregate
			ISliceFilter rawCommonFilter = filterUtility.commonAnd(filters);
			ISliceFilter commonFilter = FilterBuilder.and(rawCommonFilter).optimize(filterOptimizer);

			IFilterStripper stripper = filterStripperFactory.makeFilterStripper(commonFilter);

			ImmutableSet<String> columnsForFilters = relatedSteps.stream()
					.map(TableQueryStep::getFilter)
					.map(stripper::strip)
					.flatMap(strippedFilter -> FilterHelpers.getFilteredColumns(strippedFilter).stream())
					.collect(ImmutableSet.toImmutableSet());
			Set<String> columnsForGroupBy = relatedSteps.stream()
					.flatMap(s -> s.getGroupBy().getGroupedByColumns().stream())
					.collect(ImmutableSet.toImmutableSet());

			Map<String, IAdhocColumn> inducerGroupedByColumns = new LinkedHashMap<>();
			inducerGroupedByColumns.putAll(inducer.getGroupBy().getNameToColumn());

			Set<String> sharedColumns = Sets.union(columnsForFilters, columnsForGroupBy);

			assert inducerGroupedByColumns.keySet().containsAll(sharedColumns)
					: "InducedToInducer graph issue around inducer=%s and induced=%s".formatted(inducer, relatedSteps);
			inducerGroupedByColumns.keySet().retainAll(sharedColumns);

			TableQueryStep reforgedStep = inducer.toBuilder()
					// BEWARE the shared node should be as restrictive as possible to infer the induced
					.filter(filterOptimizer.or(filters))
					.groupBy(GroupByColumns.of(inducerGroupedByColumns.values()))
					.build();

			if (reforgedStep.equals(inducer)) {
				log.debug("We constructed the same inducer as being processed: nothing to refine");
			} else if (!relatedSteps.contains(reforgedStep)) {
				DirectedAcyclicGraph<TableQueryStep, DefaultEdge> inducedToInducer = GraphHelpers.makeGraph();

				inducedToInducer.addVertex(reforgedStep);
				log.info("Added a shared node: {} for {}", reforgedStep, relatedSteps);

				inducedToInducer.addVertex(inducer);
				// Register the new shared node to the existing induced
				inducedToInducer.addEdge(reforgedStep, inducer);
				// Register the induced to the new shared node
				relatedSteps.forEach(relatedStep -> {
					inducedToInducer.addVertex(relatedStep);
					inducedToInducer.addEdge(relatedStep, reforgedStep);
				});

				// Remove old edges
				relatedSteps.forEach(relatedStep -> {
					withFinalInducers.removeEdge(relatedStep, inducer);
				});

				// Add new edges
				Graphs.addGraph(withFinalInducers, inducedToInducer);

				break;
			}
		}
	}

	protected TableQueryStep filter(ISliceFilter commonFilter, TableQueryStep step) {
		ISliceFilter combinedFilter = FilterBuilder.and(commonFilter, step.getFilter()).optimize(filterOptimizer);
		return step.toBuilder().filter(combinedFilter).build();
	}
}
