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
import java.util.stream.Stream;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.ISinkExecutionFeedback;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.query.table.TableQueryV3;
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
public class SplitTableQueries implements IHasDagFromInducedToInducer, IHasTableQueryForSteps, ISinkExecutionFeedback {
	// From induced to inducer. Given the steps produced by the table, we may infer more steps.
	@NonNull
	DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer;

	// The nodes which are explicitly requested. Typically roots of DAG, but may also be some shared intermediate
	// nodes (if some root is an inducer of another root).
	@NonNull
	@Singular
	ImmutableSet<CubeQueryStep> explicits;

	// BEWARE The tableQuery may have lost some customMarker (e.g. as most customMarkers has no impact in
	// tableWrapper and should be ignored)
	@NonNull
	@Singular
	ImmutableMap<CubeQueryStep, TableQueryV3> stepToTables;

	@NonNull
	@Default
	Map<CubeQueryStep, SizeAndDuration> stepToCost = new ConcurrentHashMap<>();

	@Override
	public void registerExecutionFeedback(CubeQueryStep queryStep, SizeAndDuration sizeAndDuration) {
		stepToCost.put(queryStep, sizeAndDuration);
	}

	public static SplitTableQueries empty() {
		return SplitTableQueries.builder().inducedToInducer(new DirectedAcyclicGraph<>(DefaultEdge.class)).build();
	}

	@Override
	public long edgeCount() {
		return inducedToInducer.iterables().edgeCount();
	}

	@Override
	public Set<TableQueryV3> getTableQueries() {
		return ImmutableSet.copyOf(stepToTables.values());
	}

	@Override
	public Stream<StepAndFilteredAggregator> forEachCubeQuerySteps(TableQueryV3 query,
			IFilterOptimizer filterOptimizer) {
		return query.getAggregators().stream().flatMap(filteredAggregator -> {
			return query.streamGroupBy().map(groupBy -> {
				CubeQueryStep step = query.recombineQueryStep(filterOptimizer, filteredAggregator, groupBy);

				// TODO If we were to restore customMarker into steps (e.g. if suppressed by TableQueryV3), if would
				// probably happen around here

				if (containsStep(step)) {
					return new StepAndFilteredAggregator(filteredAggregator, step);
				} else {
					// TODO Should we clear some data consuming RAM?
					log.debug("Skip step as produce by table but irrelevant for cube. step={}", step);
					return null;
				}
			}).filter(Objects::nonNull);
		});
	}

	@Override
	public boolean containsStep(CubeQueryStep queryStep) {
		return stepToTables.containsKey(queryStep);
	}
}