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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.engine.concurrent.DagCompletableExecutor;
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
import eu.solven.adhoc.util.AdhocUnsafe;
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
		return filterBundle -> AddSharedNodes.builder()
				.filterStripperFactory(filterBundle.getFilterStripperFactory())
				.filterOptimizer(filterBundle.getFilterOptimizer())
				.build();
	}

	/**
	 * Given a {@link Graph}, adds intermediate nodes to share repeated computations. Independent nodes (those with no
	 * directed path between them) are processed concurrently via {@link DagCompletableExecutor} running on
	 * {@link AdhocUnsafe#adhocCpuPool}; dependency ordering is maintained so each inducer is processed only after all
	 * its induced nodes have completed.
	 *
	 * <p>
	 * A reversed snapshot of the original DAG is passed to the executor for dependency tracking. The original DAG has
	 * edges from induced→inducer; reversing it to inducer→induced lets the executor treat each inducer's predecessors
	 * as "must finish first" dependencies. The snapshot is never mutated; new shared nodes inserted into the live DAG
	 * during processing are invisible to the executor and are stabilised internally by {@link #addSharedNode}.
	 *
	 * @param input
	 *            the original DAG; never mutated
	 * @return a new DAG that may contain additional shared nodes
	 */
	@Override
	public IAdhocDag<TableQueryStep> addSharedNodes(IAdhocDag<TableQueryStep> input) {
		// Work on a copy so we never mutate the caller's graph.
		IAdhocDag<TableQueryStep> dag = GraphHelpers.copy(input);

		// Build a reversed snapshot for the executor's dependency graph (never mutated).
		// Original: induced→inducer. Reversed: inducer→induced.
		// outgoingEdgesOf(inducer) in reversed = its induced nodes = "must complete before inducer".
		IAdhocDag<TableQueryStep> reversedSnapshot = buildReversedSnapshot(dag);
		ImmutableSet<TableQueryStep> roots = GraphHelpers.getRoots(reversedSnapshot);

		ReentrantLock lock = new ReentrantLock();

		DagCompletableExecutor<TableQueryStep> executor = DagCompletableExecutor.<TableQueryStep>builder()
				.fromQueriedToDependencies(reversedSnapshot)
				// No pre-completed steps; all original nodes must be processed.
				.queryStepsDone(ConcurrentHashMap.newKeySet())
				.onReadyStep(node -> addSharedNode(lock, dag, node))
				// Forces the CPU pool as this process is CPU only
				.executor(AdhocUnsafe.adhocCpuPool)
				.build();

		executor.executeRecursively(roots).join();
		return dag;
	}

	/**
	 * Builds a reversed copy of {@code dag}: for every edge {@code u→v} in {@code dag}, the reversed graph contains
	 * {@code v→u}. The result is used as the immutable dependency graph for {@link DagCompletableExecutor}; it is never
	 * mutated after construction.
	 *
	 * @param dag
	 *            the live DAG snapshot to reverse
	 * @return a new {@link IAdhocDag} with all edges flipped
	 */
	protected IAdhocDag<TableQueryStep> buildReversedSnapshot(IAdhocDag<TableQueryStep> dag) {
		IAdhocDag<TableQueryStep> reversed = GraphHelpers.makeGraph();

		Graphs.addGraphReversed(reversed, dag);

		return reversed;
	}

	/**
	 * Exhaustively inserts shared nodes for {@code inducer} until the graph stabilises for that node. Delegates each
	 * single-insertion attempt to {@link #tryInsertSharedNode}.
	 *
	 * <p>
	 * Each successful call to {@link #tryInsertSharedNode} reduces the number of direct induced steps of the current
	 * node by at least one. The loop therefore terminates in at most {@code (initial-induced-count − 1)} iterations per
	 * node on the stack.
	 * 
	 * @param lock
	 *            used as {@link DirectedAcyclicGraph} is not thread-safe by default, while this will read-write the DAG
	 *            concurrently.
	 * @param liveDag
	 *            the live DAG being built; mutated in place
	 * @param inducer
	 *            the node whose induced set is being consolidated
	 */
	protected void addSharedNode(ReentrantLock lock, IAdhocDag<TableQueryStep> liveDag, TableQueryStep inducer) {
		Deque<TableQueryStep> toStabilise = new ArrayDeque<>();
		toStabilise.push(inducer);
		while (!toStabilise.isEmpty()) {
			TableQueryStep current = toStabilise.pop();
			Optional<TableQueryStep> newNode = tryInsertSharedNode(lock, liveDag, current);
			if (newNode.isPresent()) {
				// newNode is induced by current (child before parent in topological order), so it must
				// be stabilised first. Stack (LIFO): push current back, then newNode on top so newNode
				// is popped next; current is only retried once newNode is fully stable.
				toStabilise.push(current);
				toStabilise.push(newNode.get());
			}
		}
	}

	/**
	 * Single-insertion attempt for {@code inducer}. The method is split into three phases to allow concurrent execution
	 * when multiple independent inducers are processed in parallel by {@link DagCompletableExecutor}:
	 *
	 * <ol>
	 * <li><b>Phase 1 — synchronized read:</b> snapshot the induced set under {@code synchronized(liveDag)} to prevent
	 * concurrent modification of JGraphT's internal adjacency structures.</li>
	 * <li><b>Phase 2 — unsynchronized compute:</b> all filter analysis ({@link #makeSharedStep}, etc.) runs without
	 * holding the lock so that independent inducers execute their expensive work in parallel.</li>
	 * <li><b>Phase 3 — synchronized write:</b> graph mutations are applied under {@code synchronized(liveDag)}.
	 * Correctness holds because independent inducers only touch edges where they are the target, so their write sets
	 * are disjoint.</li>
	 * </ol>
	 * 
	 * @param lock
	 *
	 * @param liveDag
	 *            the live DAG; mutated when a shared node is inserted
	 * @param inducer
	 *            node whose induced set is being examined
	 * @return the newly inserted shared node, or {@link Optional#empty()} if the induced set is already optimal
	 */
	protected Optional<TableQueryStep> tryInsertSharedNode(ReentrantLock lock,
			IAdhocDag<TableQueryStep> liveDag,
			TableQueryStep inducer) {
		// Phase 1: synchronized snapshot of the induced set.
		ImmutableSet<TableQueryStep> inducedSteps =
				safeConcurrency(lock, () -> GraphHelpers.getInduced(liveDag, inducer));

		if (inducedSteps.size() < 2) {
			return Optional.empty();
		}

		// Phase 2: pure filter analysis — no DAG access; runs in parallel with independent inducers.
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

			// makeSharedStep is pure computation — no DAG access; runs outside the lock.
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
				// Phase 3 (promotion): independent inducers touch disjoint edges, so writes do not conflict.
				safeConcurrency(lock, () -> {
					otherSteps.forEach(s -> liveDag.removeEdge(s, inducer));
					otherSteps.forEach(s -> liveDag.addEdge(s, reforgedStep));
				});
				return Optional.of(reforgedStep);
			} else {
				// Phase 3 (new shared node): synchronized write.
				safeConcurrency(lock, () -> registerSharedInDag(liveDag, inducer, relatedSteps, reforgedStep));
				return Optional.of(reforgedStep);
			}
		}

		return Optional.empty();
	}

	protected <T> T safeConcurrency(ReentrantLock lock, Supplier<T> supplier) {
		lock.lock();
		try {
			return supplier.get();
		} finally {
			lock.unlock();
		}
	}

	protected void safeConcurrency(ReentrantLock lock, Runnable runnable) {
		lock.lock();
		try {
			runnable.run();
		} finally {
			lock.unlock();
		}
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

		assert inducerGroupedByColumns.keySet()
				.containsAll(sharedColumns) : "InducedToInducer graph issue around inducer=%s and induced=%s"
						.formatted(inducer, relatedSteps);
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
