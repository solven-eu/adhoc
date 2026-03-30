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

import java.util.Set;
import java.util.function.Function;

import org.jgrapht.Graphs;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphHelpers;
import eu.solven.adhoc.engine.tabular.optimizer.IAdhocDag;
import eu.solven.adhoc.engine.tabular.optimizer.SplitTableQueries;
import eu.solven.adhoc.engine.tabular.splitter.adder.AddSharedNodes;
import eu.solven.adhoc.engine.tabular.splitter.adder.IAddSharedNodes;
import eu.solven.adhoc.engine.tabular.splitter.merger.IMergeInducers;
import eu.solven.adhoc.engine.tabular.splitter.merger.MergeInducersStrictGroupBy;
import eu.solven.adhoc.filter.IFilterFactories;
import eu.solven.adhoc.filter.IFilterQueryBundle;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds generic behavior to {@link ITableStepsSplitter} which leads to steps being induced by Adhoc.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
public class InduceByAdhoc extends AInduceByAdhocParent {

	@Default
	@NonNull
	protected final IFilterFactories filterFactories = AdhocFactoriesUnsafe.factories;

	/**
	 * Optional query-scoped bundle. When set, it is used directly and no additional {@link IFilterQueryBundle} is
	 * created by {@link #splitInducedAsDag}, enabling the caller to share a single bundle (and its cached optimizer)
	 * across {@link InduceByAdhoc} and any surrounding
	 * {@link eu.solven.adhoc.engine.tabular.optimizer.ITableQueryFactory}. When {@code null} (the default), a fresh
	 * bundle is derived from {@link #filterFactories} on every call to {@link #splitInducedAsDag}.
	 */
	protected final IFilterQueryBundle filterBundle;

	// Holds the policy allowing one step to imply another step
	// On very large graph, we may have an heuristic policy
	@NonNull
	@Default
	protected ITableStepsSplitterFactory inferenceEdgesAdderFactory = InduceByAdhocComplete.makeFactory();

	// Holds the policy allowing to merge inducers into a different set of inducers (typically smaller in number).
	@NonNull
	@Default
	protected IMergeInducers.IMergeInducersFactory mergeInducersFactory = MergeInducersStrictGroupBy.makeFactory();

	// Holds the policy allowing to add nodes in the middle of the DAG, in order to share some computations
	@NonNull
	@Default
	protected IAddSharedNodes.IAddSharedNodesFactory sharedNodesAdderFactory = AddSharedNodes.makeFactory();

	@Override
	public IAdhocDag<TableQueryStep> splitInducedAsDag(IHasQueryOptions hasOptions,
			IAdhocDag<TableQueryStep> inducedToInducer) {
		IFilterQueryBundle queryBundle;
		if (filterBundle != null) {
			queryBundle = filterBundle;
		} else {
			queryBundle = filterFactories.makeQueryBundle();
		}

		// 1. Add inference between existing nodes
		// If we add such links, we tell the induced will be inferred by Adhoc and there will be less inducers for
		// ITableWrapper.
		// If we do not add such edges, we request the ITableWrapper to execute each TableQueryStep (e.g. as a large
		// UNION ALL)
		// BEWARE This should only add edges
		IAdhocDag<TableQueryStep> withInferenceNodes =
				inferenceEdgesAdderFactory.make(queryBundle).splitInducedAsDag(hasOptions, inducedToInducer);

		// 2. Given the new (and smaller) set of inducers, we may want to add additional vertices, merging inducers
		// together.
		ImmutableSet<TableQueryStep> tableSteps = GraphHelpers.getInducers(withInferenceNodes);

		IAdhocDag<TableQueryStep> withMergedInducers = GraphHelpers.makeGraph();
		// Step0: copy input graph
		Graphs.addGraph(withMergedInducers, inducedToInducer);
		// Step1: register the inference edges (new steps but no new vertices)
		Graphs.addGraph(withMergedInducers, withInferenceNodes);

		SetMultimap<TableQueryStep, TableQueryStep> aggregatorToQueries = groupByAggregator(tableSteps);

		// Merging inducers is done a per options+custom_marker+aggregator basis
		Multimaps.asMap(aggregatorToQueries).forEach((a, steps) -> {
			IAdhocDag<TableQueryStep> aInducedToInducer = makeMergeInducers(queryBundle).mergeInducers(a, steps);

			if (hasOptions.isDebugOrExplain()) {
				SplitTableQueries aTableQueries =
						SplitTableQueries.builder().inducedToInducer(aInducedToInducer).build();

				// TODO This log lacks options and customMarkers if any
				log.info("[EXPLAIN] explicits={} roots={} vertices={} induceds={} inducers={} for agg={}",
						steps.size(),
						aTableQueries.getRoots().size(),
						aTableQueries.getInducedToInducer().vertexSet().size(),
						aTableQueries.getInduceds().size(),
						aTableQueries.getInducers().size(),
						a.getMeasure().getName());
			}

			// Step2: merge some inducers together (new vertices and edges)
			Graphs.addGraph(withMergedInducers, aInducedToInducer);
		});

		// 3. As we created some nodes, we need to re-apply the inference between existing nodes
		// TODO Why? Need at least one example where this is relevant

		// 4. Now, we want to add some additional sharing nodes: these are never inducers but in the middle of the DAG.
		// They will help computing only once elements of inference (e.g. some filter or some groupBy)

		// add shared nodes over the full graph, as both explicit and other steps may benefit from shared nodes
		IAdhocDag<TableQueryStep> sharedNodes = addSharedNodes(queryBundle, withMergedInducers);
		Graphs.addGraph(withMergedInducers, sharedNodes);
		return withMergedInducers;
	}

	/**
	 * A last-minute step typically used to add shared nodes in the middle of the DAG, to help applying some
	 * computations only once.
	 *
	 * @param queryBundle
	 *            query-scoped filter tools
	 * @param inducedToInducer
	 *            current DAG
	 * @return a DAG fragment containing any newly added shared nodes
	 */
	protected IAdhocDag<TableQueryStep> addSharedNodes(IFilterQueryBundle queryBundle,
			IAdhocDag<TableQueryStep> inducedToInducer) {
		return makeSharedNodesAdder(queryBundle).addSharedNodes(inducedToInducer);
	}

	protected SetMultimap<TableQueryStep, TableQueryStep> groupByAggregator(Set<TableQueryStep> steps) {
		return groupBy(steps, step -> {
			// Typically holds options and customMarkers
			TableQueryStep contextOnly = contextOnly(step);

			return TableQueryStep.edit(contextOnly).aggregator(step.getMeasure()).build();
		});
	}

	protected SetMultimap<TableQueryStep, TableQueryStep> groupBy(Set<TableQueryStep> steps,
			Function<TableQueryStep, TableQueryStep> toGroup) {
		return steps.stream()
				.collect(Multimaps.toMultimap(toGroup::apply, Function.identity(), LinkedHashMultimap::create));
	}

	/**
	 * Creates the {@link IMergeInducers} for a single query execution.
	 *
	 * @param queryBundle
	 *            query-scoped filter tools
	 * @return a configured merger
	 */
	protected IMergeInducers makeMergeInducers(IFilterQueryBundle queryBundle) {
		return mergeInducersFactory.makeMergeInducer(queryBundle);
	}

	/**
	 * Creates the {@link IAddSharedNodes} for a single query execution.
	 *
	 * @param queryBundle
	 *            query-scoped filter tools
	 * @return a configured shared-node adder
	 */
	protected IAddSharedNodes makeSharedNodesAdder(IFilterQueryBundle queryBundle) {
		return sharedNodesAdderFactory.make(queryBundle);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("inferenceEdgesAdderFactory", inferenceEdgesAdderFactory)
				.add("mergeInducersFactory", mergeInducersFactory)
				.add("sharedNodesAdderFactory", sharedNodesAdderFactory)
				.toString();
	}
}
