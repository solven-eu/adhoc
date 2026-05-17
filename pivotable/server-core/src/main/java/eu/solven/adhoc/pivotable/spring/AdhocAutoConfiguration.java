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
package eu.solven.adhoc.pivotable.spring;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.engine.observability.plan.BoundedQueryPlanRegistry;
import eu.solven.adhoc.engine.observability.plan.IQueryPlanRegistry;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.eventbus.UnsafeAdhocEventBusHelpers;
import eu.solven.adhoc.factories.AdhocFactories;
import eu.solven.adhoc.factories.AdhocFactoriesUnsafe;
import eu.solven.adhoc.factories.IAdhocFactories;
import eu.solven.adhoc.factories.IColumnFactory;
import eu.solven.adhoc.factories.StandardColumnFactory;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizerFactory;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.ISliceFactoryFactory;
import eu.solven.adhoc.map.factory.RowSliceFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.util.IStopwatchFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * The beans needed to instantiate Adhoc components
 * 
 * @author Benoit Lacelle
 */
@AutoConfiguration
@Slf4j
public class AdhocAutoConfiguration {

	/** Default cap on the in-memory plan registry — enough for ~10 simultaneous 20k-step plans. */
	private static final long DEFAULT_PLAN_REGISTRY_NODE_BUDGET = 200_000L;

	@Bean
	@ConditionalOnMissingBean({ IAdhocEventBus.class,
			com.google.common.eventbus.EventBus.class,
			org.greenrobot.eventbus.EventBus.class })
	public com.google.common.eventbus.EventBus adhocEventBus() {
		log.info("Autoconfigured: {}", com.google.common.eventbus.EventBus.class.getName());
		return new com.google.common.eventbus.EventBus("Adhoc");
	}

	@Bean
	@ConditionalOnMissingBean(IAdhocEventBus.class)
	@ConditionalOnBean(com.google.common.eventbus.EventBus.class)
	public IAdhocEventBus adhocEventBusFromGuava(com.google.common.eventbus.EventBus eventBus) {
		log.info("Autoconfigured: IAdhocEventBus over {}", eventBus.getClass().getName());
		return UnsafeAdhocEventBusHelpers.safeWrapper(eventBus::post);
	}

	@Bean
	@ConditionalOnMissingBean(IAdhocEventBus.class)
	@ConditionalOnBean(org.greenrobot.eventbus.EventBus.class)
	public IAdhocEventBus adhocEventBusFromGreenRobot(org.greenrobot.eventbus.EventBus eventBus) {
		log.info("Autoconfigured: IAdhocEventBus over {}", eventBus.getClass().getName());
		return UnsafeAdhocEventBusHelpers.safeWrapper(eventBus::post);
	}

	@Bean
	@ConditionalOnMissingBean(IOperatorFactory.class)
	public IOperatorFactory adhocOperatorsFactory() {
		return StandardOperatorFactory.builder().build();
	}

	@Bean
	@ConditionalOnMissingBean(IColumnFactory.class)
	public IColumnFactory columnFactory() {
		return StandardColumnFactory.builder().build();
	}

	// BEWARE As this is a shared ISliceFactory, it must not keep memory in the long-run (e.g. through dictionaries)
	@Bean
	@ConditionalOnMissingBean(ISliceFactory.class)
	public ISliceFactory sliceFactory() {
		return RowSliceFactory.builder().build();
	}

	@Bean
	@ConditionalOnMissingBean(ISliceFactoryFactory.class)
	public ISliceFactoryFactory sliceFactoryFactory() {
		return AdhocFactoriesUnsafe.factories.getSliceFactoryFactory();
	}

	@Bean
	@ConditionalOnMissingBean(IStopwatchFactory.class)
	public IStopwatchFactory stopwatchFactory() {
		return IStopwatchFactory.guavaStopwatchFactory();
	}

	@Bean
	@ConditionalOnMissingBean(IFilterOptimizerFactory.class)
	public IFilterOptimizerFactory filterOptimizerFactory() {
		return IFilterOptimizerFactory.standard();
	}

	@Bean
	@ConditionalOnMissingBean(IFilterStripperFactory.class)
	public IFilterStripperFactory filterStripperFactory() {
		// TODO Should we rely on Unsafe?
		return IFilterStripperFactory.noCache();
	}

	@Bean
	@ConditionalOnMissingBean(IAdhocFactories.class)
	public IAdhocFactories adhocFactories(IOperatorFactory operatorFactory,
			IFilterOptimizerFactory filterOptimizerFactory,
			IFilterStripperFactory filterStripperFactory,
			IColumnFactory columnsFactory,
			ISliceFactoryFactory sliceFactoryFactory,
			IStopwatchFactory stopwatchFactory) {
		// TODO Should we rely on Unsafe?
		return AdhocFactories.builder()
				.columnFactory(columnsFactory)
				.filterOptimizerFactory(filterOptimizerFactory)
				.filterStripperFactory(filterStripperFactory)
				.operatorFactory(operatorFactory)
				.sliceFactoryFactory(sliceFactoryFactory)
				.stopwatchFactory(stopwatchFactory)
				.build();
	}

	/**
	 * In-memory plan registry, capped by total live node count. Pivotable's UI Live View polls this to render an "what
	 * is the query doing right now" indicator. The default cap of 200k nodes is large enough to retain a handful of
	 * 20k-step plans simultaneously without unbounded growth.
	 *
	 * @return a {@link BoundedQueryPlanRegistry}; users can override with their own bean (e.g. a no-op for production
	 *         setups that don't need the feature).
	 */
	@Bean
	@ConditionalOnMissingBean(IQueryPlanRegistry.class)
	public IQueryPlanRegistry queryPlanRegistry() {
		return new BoundedQueryPlanRegistry(DEFAULT_PLAN_REGISTRY_NODE_BUDGET);
	}

	@Bean
	@ConditionalOnMissingBean(ICubeQueryEngine.class)
	public ICubeQueryEngine adhocQueryEngine(IAdhocEventBus eventBus, IAdhocFactories adhocFactories) {
		// {@link IQueryPlanRegistry} is no longer threaded into the engine — the preparator (built by
		// {@link AdhocSchema}) sets it on the {@code QueryPod}, and the engine reads it from there. Callers that
		// register a custom registry bean get it threaded through {@link IAdhocSchemaCustomizer} (see Pivotable's
		// {@code InjectPivotableSelfEndpointConfig}).
		return CubeQueryEngine.builder().eventBus(eventBus).factories(adhocFactories).build();
	}
}
