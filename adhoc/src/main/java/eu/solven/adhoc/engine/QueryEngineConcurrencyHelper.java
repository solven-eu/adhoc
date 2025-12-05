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
package eu.solven.adhoc.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Consumer;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.engine.QueryStepRecursiveAction.QueryStepRecursiveActionBuilder;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.IHasDagFromInducedToInducer;
import eu.solven.adhoc.query.StandardQueryOptions;
import lombok.experimental.UtilityClass;

/**
 * Helps managing concurrency in different query engines.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class QueryEngineConcurrencyHelper {

	/**
	 * Execute the steps as described by a DAG.
	 * 
	 * This handles cancellation as described by QueryPod.
	 * 
	 * @param queryPod
	 * @param queryStepsDag
	 * @param queryStepToValues
	 * @param queryStepConsumer
	 */
	public static void walkUpDag(QueryPod queryPod,
			IHasDagFromInducedToInducer queryStepsDag,
			Map<CubeQueryStep, ISliceToValue> queryStepToValues,
			Consumer<? super CubeQueryStep> queryStepConsumer) {
		Consumer<? super CubeQueryStep> cancellableStepConsumer = step -> {
			if (queryPod.isCancelled()) {
				throw new CancelledQueryException("queryPod is cancelled. Not starting step=%s".formatted(step));
			}

			try {
				queryStepConsumer.accept(step);
			} finally {
				CancellationHelpers.afterCancellable(queryPod);
			}

		};

		ListenableFuture<?> future = queryPod.getExecutorService().submit(() -> {
			if (queryPod.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
				// multi-threaded
				DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = queryStepsDag.getInducedToInducer();

				// list root induced
				List<CubeQueryStep> rootInduced =
						dag.vertexSet().stream().filter(step -> dag.inDegreeOf(step) == 0).toList();

				QueryStepRecursiveActionBuilder actionTemplate = QueryStepRecursiveAction.builder()
						.fromQueriedToDependancies(dag)
						.queryStepToValues(queryStepToValues)
						.onReadyStep(cancellableStepConsumer);
				List<QueryStepRecursiveAction> rootActions =
						rootInduced.stream().map(step -> actionTemplate.step(step).build()).toList();

				List<Runnable> stepCancellationListeners = new ArrayList<>();
				rootActions.forEach(action -> {
					stepCancellationListeners.add(() -> action.cancel(true));
				});
				stepCancellationListeners.forEach(queryPod::addCancellationListener);

				// BEWARE: In case of exception, is it important to remove the cancellation listeners?
				// try {
				ForkJoinTask.invokeAll(rootActions);
				// } finally {
				// tasksCancellationListeners.forEach(queryPod::removeCancellationListener);
				// }
			} else {
				// mono-threaded
				queryStepsDag.iteratorFromInducerToInduced().forEachRemaining(cancellableStepConsumer);
			}
		});

		Runnable cancellationListener = () -> {
			future.cancel(true);
		};

		queryPod.addCancellationListener(cancellationListener);
		try {
			Futures.getUnchecked(future);
		} finally {
			CancellationHelpers.afterCancellable(queryPod);
		}
		queryPod.removeCancellationListener(cancellationListener);
	}

}
