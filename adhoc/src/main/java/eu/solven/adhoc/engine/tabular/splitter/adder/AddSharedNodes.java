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
import java.util.Optional;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.traverse.TopologicalOrderIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphHelpers;
import eu.solven.adhoc.engine.tabular.optimizer.IAdhocDag;
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
	 * Given a {@link Graph}, adds intermediate nodes to share repeated computations. For example, if two nodes depend
	 * on the same inducer and apply overlapping {@link ISliceFilter}s, an intermediate node is inserted that applies
	 * the common filter, letting both dependants reuse the result.
	 *
	 * <p>
	 * <b>Single linear pass.</b> The traversal order is fixed upfront from a snapshot of the DAG taken before any
	 * mutation. {@link TopologicalOrderIterator} must not be used while the graph is being mutated (JGraphT contract),
	 * so its output is captured into an {@link ImmutableList} before the first insertion.
	 *
	 * <p>
	 * Two properties justify why one forward pass over this list is sufficient:
	 * <ol>
	 * <li><b>Newly inserted shared nodes are absent from the list, but are processed eagerly.</b>
	 * {@link #tryInsertSharedNode} returns the newly created node, and {@link #addSharedNode} immediately recurses into
	 * it before continuing its own loop. This ensures every new node is fully stabilised (all sub-sharing opportunities
	 * among its children are exhausted) before control returns to the outer iteration.</li>
	 * <li><b>The parent of each newly inserted shared node is already present later in the list</b> at a higher
	 * topological position (closer to the root). It will be processed in the natural forward iteration, finding the
	 * already-inserted shared node as part of its updated induced set — no explicit re-queuing is needed.</li>
	 * </ol>
	 *
	 * @param input
	 *            the original DAG; never mutated
	 * @return a new DAG that may contain additional shared nodes
	 */
	@Override
	public IAdhocDag<TableQueryStep> addSharedNodes(IAdhocDag<TableQueryStep> input) {
		// Work on a copy so we never mutate the caller's graph
		IAdhocDag<TableQueryStep> dag = GraphHelpers.copy(input);

		// Snapshot the traversal order before any DAG mutations.
		// Topological order (inDegree=0 leaves first, outDegree=0 roots last) ensures each
		// node is encountered only after all of its induced children have been fully processed.
		List<TableQueryStep> toProcess = ImmutableList.copyOf(new TopologicalOrderIterator<>(dag));

		for (TableQueryStep inducer : toProcess) {
			if (!dag.containsVertex(inducer)) {
				// Defensive: vertex removed during a prior iteration (e.g. subclass override).
				continue;
			}
			addSharedNode(dag, inducer);
		}

		return dag;
	}

	/**
	 * Exhaustively inserts shared nodes for {@code inducer} until the graph stabilises for that node. Delegates each
	 * single-insertion attempt to {@link #tryInsertSharedNode}; after each successful insertion it recurses into the
	 * newly created shared node to stabilise it before continuing, then re-examines {@code inducer}'s updated induced
	 * set.
	 *
	 * <p>
	 * Each successful call to {@link #tryInsertSharedNode} reduces the number of direct induced steps of
	 * {@code inducer} by at least one. The recursion therefore terminates in at most
	 * {@code (initial-induced-count − 1)} calls.
	 *
	 * @param withFinalInducers
	 *            the live DAG being built; mutated in place
	 * @param inducer
	 *            the node whose induced set is being consolidated
	 */
	protected void addSharedNode(IAdhocDag<TableQueryStep> withFinalInducers, TableQueryStep inducer) {
		while (true) {
			// Re-apply on inducer until there is no more shared node to add
			Optional<TableQueryStep> newNode = tryInsertSharedNode(withFinalInducers, inducer);
			if (newNode.isEmpty()) {
				break;
			}
			// Stabilise the newly created shared node before re-examining inducer's updated induced set.
			newNode.ifPresent(n -> addSharedNode(withFinalInducers, n));
		}
	}

	/**
	 * Single-insertion attempt: scans the current induced steps of {@code inducer} for a filter part shared by at least
	 * two of them, constructs a shared node whose filter is the OR of those steps' filters, and rewires the graph.
	 *
	 * <p>
	 * Only one shared node is inserted per call. The caller ({@link #addSharedNode}) is responsible for looping and for
	 * recursively stabilising the returned node.
	 *
	 * @param withFinalInducers
	 *            the live DAG; mutated when a shared node is inserted
	 * @param inducer
	 *            node whose induced set is being examined
	 * @return the newly inserted shared node, or {@link Optional#empty()} if the induced set is already optimal
	 */
	protected Optional<TableQueryStep> tryInsertSharedNode(IAdhocDag<TableQueryStep> withFinalInducers,
			TableQueryStep inducer) {
		ImmutableSet<TableQueryStep> inducedSteps = GraphHelpers.getInduced(withFinalInducers, inducer);

		if (inducedSteps.size() < 2) {
			return Optional.empty();
		}

		// `a&b|a&b|a&c|d` should lead to `a&(b|c)|d` to the tableQuery
		// and `a&(b|c)` as intermediate step, useful to prepare both `a&b` and `a&c` steps
		// and `a` as intermediate step, useful to prepare `a&b` and `a&c`
		// BEWARE AtomicLongMap is not ordered but we later sort by a comparator
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

		FilterUtility filterUtility = FilterUtility.builder().optimizer(filterOptimizer).build();

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
			assert relatedSteps.size() == mostPresentPart.getValue();

			TableQueryStep reforgedStep = makeSharedStep(inducer, relatedSteps, filterUtility);

			if (reforgedStep.equals(inducer)) {
				log.debug("We constructed the same inducer as being processed: already optimal parent");
			} else if (relatedSteps.contains(reforgedStep)) {
				// reforgedStep already exists as one of the induced steps and its filter equals
				// OR(all related filters): it is the broadest step in the group. Promote it to
				// shared-node role by rewiring the other related steps through it; its own edge
				// to inducer stays unchanged.
				ImmutableSet<TableQueryStep> otherSteps = relatedSteps.stream()
						.filter(s -> !s.equals(reforgedStep))
						.collect(ImmutableSet.toImmutableSet());
				log.debug("Promoting existing induced step {} as shared node for {}", reforgedStep, otherSteps);
				otherSteps.forEach(s -> withFinalInducers.removeEdge(s, inducer));
				otherSteps.forEach(s -> withFinalInducers.addEdge(s, reforgedStep));
				return Optional.of(reforgedStep);
			} else {
				registerSharedInDag(withFinalInducers, inducer, relatedSteps, reforgedStep);

				return Optional.of(reforgedStep);
			}
		}

		return Optional.empty();
	}

	protected void registerSharedInDag(IAdhocDag<TableQueryStep> withFinalInducers,
			TableQueryStep inducer,
			ImmutableSet<TableQueryStep> relatedSteps,
			TableQueryStep reforgedStep) {
		IAdhocDag<TableQueryStep> inducedToInducer = GraphHelpers.makeGraph();

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
	}

	protected TableQueryStep makeSharedStep(TableQueryStep inducer,
			ImmutableSet<TableQueryStep> relatedSteps,
			FilterUtility filterUtility) {
		Set<? extends ISliceFilter> filters =
				relatedSteps.stream().map(TableQueryStep::getFilter).collect(ImmutableSet.toImmutableSet());

		// Evaluate the `WHERE` common to all steps of given aggregate
		ISliceFilter rawCommonFilter = filterUtility.commonAnd(filters);
		ISliceFilter commonFilter = FilterBuilder.and(rawCommonFilter).optimize(filterOptimizer);

		IFilterStripper stripper = filterStripperFactory.makeFilterStripper(commonFilter);

		ImmutableSet<String> columnsForFilters = relatedSteps.stream()
				.map(TableQueryStep::getFilter)
				.map(stripper::strip)
				.flatMap(FilterHelpers::streamFilteredColumns)
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

		return inducer.toBuilder()
				// BEWARE the shared node should be as restrictive as possible to infer the induced
				.filter(filterOptimizer.or(filters, false))
				.groupBy(GroupByColumns.of(inducerGroupedByColumns.values()))
				.build();
	}

	/**
	 * Combines {@code commonFilter} with the filter of {@code step} and returns a new step carrying the resulting
	 * AND-filter. Exposed as a protected hook for subclasses that need to customise how filters are merged.
	 *
	 * @param commonFilter
	 *            the filter part to AND into {@code step}
	 * @param step
	 *            the step whose filter is being narrowed
	 * @return a copy of {@code step} with the combined filter
	 */
	protected TableQueryStep filter(ISliceFilter commonFilter, TableQueryStep step) {
		ISliceFilter combinedFilter = FilterBuilder.and(commonFilter, step.getFilter()).optimize(filterOptimizer);
		return step.toBuilder().filter(combinedFilter).build();
	}
}
