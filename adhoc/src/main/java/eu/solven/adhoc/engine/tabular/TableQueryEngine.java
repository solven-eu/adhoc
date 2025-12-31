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
package eu.solven.adhoc.engine.tabular;

import java.util.Map;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizerFactory;
import eu.solven.adhoc.engine.tabular.optimizer.TableQueryOptimizerFactory;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.eventbus.UnsafeAdhocEventBusHelpers;
import eu.solven.adhoc.util.AdhocBlackHole;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Default {@link ITableQueryEngine}.
 * 
 * It implements optimizations of the queryPlans projected to the underlying table. Typically:
 * 
 * <ul>
 * <li>Request for `m over c=c1&d=d1` and `m over c=c1&d=d2` should request `m(FILTER d=d1) and m(FILTER d=d2) over
 * c=c1`</li>
 * <li>Request for `m groupBy c` and `m groupBy c&d` should request `m groupBy c,c&d`</li>
 * </ul>
 * 
 * @author Benoit Lacelle
 */
@Builder(toBuilder = true)
@Slf4j
public class TableQueryEngine implements ITableQueryEngine {

	@NonNull
	@Default
	@Getter
	final AdhocFactories factories = AdhocFactories.builder().build();

	@NonNull
	@Default
	final IAdhocEventBus eventBus = UnsafeAdhocEventBusHelpers.safeWrapper(AdhocBlackHole.getInstance());

	@NonNull
	@Default
	final ITableQueryOptimizerFactory optimizerFactory = new TableQueryOptimizerFactory();

	@Override
	public Map<CubeQueryStep, ISliceToValue> executeTableQueries(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		return bootstrap(queryPod).executeTableQueries(queryStepsDag);
	}

	protected TableQueryEngineBootstrapped bootstrap(QueryPod queryPod) {
		ITableQueryOptimizer optimizer = optimizerFactory.makeOptimizer(factories, queryPod);

		if (queryPod.isDebugOrExplain()) {
			log.info("[EXPLAIN] Using optimizer={} for query={}", optimizer, queryPod.getQueryId());
		}

		return bootstrap(queryPod, optimizer);
	}

	protected TableQueryEngineBootstrapped bootstrap(QueryPod queryPod, ITableQueryOptimizer optimizer) {
		return new TableQueryEngineBootstrapped(factories, eventBus, queryPod, optimizer);
	}

}
