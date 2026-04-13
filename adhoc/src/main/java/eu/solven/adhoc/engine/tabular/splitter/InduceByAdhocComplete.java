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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jgrapht.Graphs;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import eu.solven.adhoc.engine.dag.GraphHelpers;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.stripper.IFilterStripper;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.jgrapht.alg.TransitiveReductionV2;
import eu.solven.adhoc.options.IHasOptionsAndExecutorService;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Relates with {@link InduceByAdhocOptimistic} by registering all inducing options from one step to another (and not
 * picking one which may be the best a priori),
 *
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
@RequiredArgsConstructor
public class InduceByAdhocComplete extends AInduceByAdhocParent implements IAddOnlyEdges {

	@NonNull
	@Default
	@Getter
	final IFilterStripperFactory filterStripperFactory = AdhocFactoriesUnsafe.factories.getFilterStripperFactory();

	/**
	 * Given an input set of steps, callback on edge of an inducing DAG. The returned DAG looks for the edge which are
	 * the simplest one (e.g. given `a`, `a,b` and `a,b,c`, while `a` can induce both `a,b` and `a,b,c`, we prefer to
	 * have `a,b` as inducer of `a,b,c`).
	 *
	 * This minimizing strategy helps minimizing the actual computations when reducing (e.g. it is easier to infer `a`
	 * given `a,b` than given `a,b,c`).
	 *
	 * @param hasOptions
	 * @param inducedToInducer
	 * @return
	 */
	@Override
	@SuppressWarnings("PMD.CloseResource")
	public IAdhocDag<TableQueryStep> splitInducedAsDag(IHasOptionsAndExecutorService hasOptions,
			IAdhocDag<TableQueryStep> inducedToInducer) {
		Set<TableQueryStep> steps = inducedToInducer.vertexSet();
		if (steps.isEmpty()) {
			return GraphHelpers.makeGraph();
		}

		IAdhocDag<TableQueryStep> induceByAdhoc = GraphHelpers.makeGraph();

		// Phase 1: group by measure name, then by context (options + customMarker).
		// Steps from different measures or contexts can never induce each other, so we avoid evaluating those pairs.
		Map<String, List<TableQueryStep>> byMeasure = steps.stream()
				.collect(Collectors.groupingBy(s -> s.getMeasure().getName(), LinkedHashMap::new, Collectors.toList()));

		// Enables cache sharing
		IFilterStripper sharedStripper = filterStripperFactory.makeFilterStripper(ISliceFilter.MATCH_ALL);
		InduceByAdhocCompleteInner complete = new InduceByAdhocCompleteInner(sharedStripper::withWhere);
		ListeningExecutorService les = hasOptions.getExecutorService();

		// Concurrent path: each context group is processed in its own local DAG, then merged.
		List<ListenableFuture<IAdhocDag<TableQueryStep>>> futures =
				byMeasure.values().stream().flatMap(measureSteps -> {
					Map<TableQueryStep, List<TableQueryStep>> byContext = measureSteps.stream()
							.collect(Collectors.groupingBy(this::contextOnly, LinkedHashMap::new, Collectors.toList()));

					return byContext.values()
							.stream()
							.map(contextSteps -> les
									.submit(() -> complete.buildEdgesForContextGroupLocal(contextSteps)));
				}).toList();
		List<IAdhocDag<TableQueryStep>> localDags = Futures.getUnchecked(Futures.allAsList(futures));
		localDags.forEach(localDag -> Graphs.addGraph(induceByAdhoc, localDag));

		// Remove transitive edges: e.g. given a->b->c, the a->c edge is redundant
		TransitiveReductionV2.INSTANCE.reduce(induceByAdhoc);

		return induceByAdhoc;
	}

	public static ITableStepsSplitterFactory makeFactory() {
		return filterBundle -> InduceByAdhocComplete.builder()
				.filterStripperFactory(filterBundle.getFilterStripperFactory())
				.build();
	}
}
