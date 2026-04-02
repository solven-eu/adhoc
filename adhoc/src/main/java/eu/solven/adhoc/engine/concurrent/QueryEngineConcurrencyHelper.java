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
package eu.solven.adhoc.engine.concurrent;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.cancel.CancellationHelpers;
import eu.solven.adhoc.engine.cancel.CancelledQueryException;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.step.ICubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphHelpers;
import eu.solven.adhoc.engine.tabular.optimizer.IAdhocDag;
import eu.solven.adhoc.engine.tabular.optimizer.IHasDagFromInducedToInducer;
import eu.solven.adhoc.options.StandardQueryOptions;
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
	public static <T extends ICubeQueryStep> void walkUpDag(QueryPod queryPod,
			IHasDagFromInducedToInducer<T> queryStepsDag,
			Map<T, ICuboid> queryStepToValues,
			Consumer<? super T> queryStepConsumer) {
		Consumer<? super T> cancellableStepConsumer = step -> {
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
				IAdhocDag<T> dag = queryStepsDag.getInducedToInducer();

				invokeDagFromRoots(queryPod, queryStepToValues.keySet(), cancellableStepConsumer, dag);
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

	private static <T extends ICubeQueryStep> void invokeDagFromRoots(QueryPod queryPod,
			Set<T> queryStepsDone,
			Consumer<? super T> onReadyStep,
			IAdhocDag<T> dag) {
		// list roots
		ImmutableSet<T> rootSteps = GraphHelpers.getRoots(dag);

		DagCompletableExecutor<T> executor = DagCompletableExecutor.<T>builder()
				.fromQueriedToDependencies(dag)
				.queryStepsDone(queryStepsDone)
				.onReadyStep(onReadyStep)
				.executor(queryPod.getExecutorService())
				.hasOptions(queryPod)
				.build();

		CompletableFuture<Void> root = executor.executeRecursively(rootSteps);

		// wait for the whole DAG
		root.join();
	}

}
