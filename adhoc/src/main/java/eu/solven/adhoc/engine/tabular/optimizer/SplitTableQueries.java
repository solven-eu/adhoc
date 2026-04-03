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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;

import eu.solven.adhoc.engine.ISinkExecutionFeedback;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.dag.GraphHelpers;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ICubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Inducers {@link CubeQueryStep} are evaluated by a {@link ITableWrapper}.
 * 
 * Induced {@link CubeQueryStep} are evaluated given the results associated to the inducers.
 * 
 * @author Benoit Lacelle
 * @see QueryStepsDag
 */
@Value
@Builder(toBuilder = true)
@Slf4j
public class SplitTableQueries
		implements IHasDagFromInducedToInducer<TableQueryStep>, IHasTableQueryForSteps, ISinkExecutionFeedback {
	// From induced to inducer. Given the steps produced by the table, we may infer more steps.
	@NonNull
	IAdhocDag<TableQueryStep> inducedToInducer;

	// The nodes which are explicitly requested. Typically roots of DAG, but may also be some shared intermediate
	// nodes (if some root is an inducer of another root).
	@NonNull
	@Singular
	ImmutableSet<TableQueryStep> explicits;

	// BEWARE The tableQuery may have lost some customMarker (e.g. as most customMarkers has no impact in
	// tableWrapper and should be ignored)
	@NonNull
	@Singular
	ImmutableMap<TableQueryStep, TableQueryV4> stepToTables;

	@NonNull
	// @Default
	Function<ListeningExecutorService, IAdhocDag<TableQueryStep>> lazyGraph
	// = __ -> getInducedToInducer()
	;

	@NonNull
	@Default
	Map<ICubeQueryStep, SizeAndDuration> stepToCost = new ConcurrentHashMap<>();

	@Override
	public void registerExecutionFeedback(ICubeQueryStep queryStep, SizeAndDuration sizeAndDuration) {
		stepToCost.put(queryStep, sizeAndDuration);
	}

	public static SplitTableQueries empty() {
		IAdhocDag<TableQueryStep> dag = GraphHelpers.makeGraph();
		return SplitTableQueries.builder().inducedToInducer(dag).lazyGraph(les -> GraphHelpers.immutable(dag)).build();
	}

	@Override
	public long edgeCount() {
		return inducedToInducer.iterables().edgeCount();
	}

	@Override
	public Set<TableQueryV4> getTableQueries() {
		return ImmutableSet.copyOf(stepToTables.values());
	}

	@Override
	public Stream<StepAndFilteredAggregator> forEachCubeQuerySteps(TableQueryV4 query,
			IFilterOptimizer filterOptimizer) {
		return query.getGroupByToAggregators().entries().stream().map(e -> {
			FilteredAggregator filteredAggregator = e.getValue();
			IGroupBy groupBy = e.getKey();
			TableQueryStep cubeStep = query.recombineQueryStep(filterOptimizer, filteredAggregator, groupBy);

			if (containsStep(cubeStep)) {
				return new StepAndFilteredAggregator(filteredAggregator, cubeStep);
			} else {
				log.debug("Skip step as produce by table but irrelevant for cube. step={}", cubeStep);
				return null;
			}
		}).filter(Objects::nonNull);
	}

	@Override
	public boolean containsStep(TableQueryStep queryStep) {
		return stepToTables.containsKey(queryStep);
	}

}