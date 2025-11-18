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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.ISinkExecutionFeedback;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * {@link ITableQueryOptimizer} will turn an input {@link Set} of {@link TableQuery} into a {@link SplitTableQueries},
 * telling which {@link TableQuery} will be executed and how to evaluate the other {@link TableQuery} from the results
 * of the later.
 * 
 * The inducer may or may not be amongst the provided input {@link TableQuery}.
 * 
 * @author Benoit Lacelle
 */
public interface ITableQueryOptimizer {

	/**
	 * Inducers {@link TableQuery} are evaluated by a {@link ITableWrapper}.
	 * 
	 * Induced {@link TableQuery} are evaluated given the results associated to the inducers.
	 * 
	 * @author Benoit Lacelle
	 * @see QueryStepsDag
	 */
	@Value
	@Builder
	class SplitTableQueries implements IHasDagFromInducedToInducer, ISinkExecutionFeedback {
		// From induced to inducer
		@NonNull
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducer;

		@NonNull
		@Singular
		ImmutableSet<CubeQueryStep> explicits;

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
	}

	/**
	 * 
	 * @param tableQuerySteps
	 *            the {@link CubeQueryStep} needed to be evaluated by the {@link ITableWrapper}
	 * @return an {@link SplitTableQueries} defining a {@link Set} of {@link TableQuery} from which all necessary
	 *         {@link TableQuery} can not be induced.
	 */
	SplitTableQueries splitInduced(IHasQueryOptions hasOptions, Set<CubeQueryStep> tableQuerySteps);

	@Deprecated(since = ".splitInduced", forRemoval = true)
	default SplitTableQueries splitInducedLegacy(IHasQueryOptions hasOptions, Set<TableQuery> tableQueries) {
		Set<CubeQueryStep> steps = tableQueries.stream().flatMap(tq -> {
			return tq.getAggregators().stream().map(agg -> CubeQueryStep.edit(tq).measure(agg).build());
		}).collect(ImmutableSet.toImmutableSet());

		return splitInduced(hasOptions, steps);
	}

	Set<TableQueryV2> packStepsIntoTableQueries(ITableWrapper tableWrapper, Set<CubeQueryStep> tableQueries);

	/**
	 * 
	 * @param hasOptions
	 * @param inducerAndInduced
	 * @param stepToValues
	 * @param induced
	 * @return a {@link IMultitypeMergeableColumn} holding the result for given induced step
	 */
	IMultitypeMergeableColumn<IAdhocSlice> evaluateInduced(IHasQueryOptions hasOptions,
			SplitTableQueries inducerAndInduced,
			Map<CubeQueryStep, ISliceToValue> stepToValues,
			CubeQueryStep induced);

}
