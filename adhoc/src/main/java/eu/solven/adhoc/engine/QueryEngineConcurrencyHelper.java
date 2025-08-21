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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Consumer;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.engine.QueryStepRecursiveAction.QueryStepRecursiveActionBuilder;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.IHasDagFromQueriedToUnderlyings;
import eu.solven.adhoc.query.StandardQueryOptions;
import lombok.experimental.UtilityClass;

/**
 * Helps managing concurrency in different query engines.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class QueryEngineConcurrencyHelper {

	public static void walkUpDag(QueryPod queryPod,
			IHasDagFromQueriedToUnderlyings queryStepsDag,
			Map<CubeQueryStep, ISliceToValue> queryStepToValues,
			Consumer<? super CubeQueryStep> queryStepConsumer) {
		try {
			queryPod.getExecutorService().submit(() -> {
				if (queryPod.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
					// multi-threaded
					DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = queryStepsDag.getDagToDependancies();

					List<CubeQueryStep> roots =
							dag.vertexSet().stream().filter(step -> dag.getAncestors(step).isEmpty()).toList();

					QueryStepRecursiveActionBuilder actionTemplate = QueryStepRecursiveAction.builder()
							.fromQueriedToDependancies(dag)
							.queryStepToValues(queryStepToValues)
							.onReadyStep(queryStepConsumer);
					List<QueryStepRecursiveAction> actions =
							roots.stream().map(step -> actionTemplate.step(step).build()).toList();

					ForkJoinTask.invokeAll(actions);
				} else {
					// mono-threaded
					queryStepsDag.iteratorFromUnderlyingsToQueried().forEachRemaining(queryStepConsumer);
				}

			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted", e);
		} catch (ExecutionException e) {
			throw new IllegalStateException("Failed", e);
		}
	}

}
