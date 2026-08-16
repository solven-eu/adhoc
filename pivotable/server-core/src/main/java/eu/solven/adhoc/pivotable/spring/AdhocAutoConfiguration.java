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
import org.springframework.core.env.Environment;

import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.engine.cache.GuavaQueryStepCache;
import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.engine.context.IImplicitFilter;
import eu.solven.adhoc.engine.context.IImplicitOptions;
import eu.solven.adhoc.engine.context.IQueryPreparator;
import eu.solven.adhoc.engine.context.SpringImplicitOptions;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.engine.observability.plan.BoundedQueryPlanRegistry;
import eu.solven.adhoc.engine.observability.plan.IQueryPlanRegistry;
import eu.solven.adhoc.eventbus.AdhocEventBusHelpersUnsafe;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.factories.IAdhocFactories;
import eu.solven.adhoc.filter.ISliceFilter;
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
	private static final long DEFAULT_QUERY_STEP_CACHE_BUDGET = 20_000L;

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
		return AdhocEventBusHelpersUnsafe.safeWrapper(eventBus::post);
	}

	@Bean
	@ConditionalOnMissingBean(IAdhocEventBus.class)
	@ConditionalOnBean(org.greenrobot.eventbus.EventBus.class)
	public IAdhocEventBus adhocEventBusFromGreenRobot(org.greenrobot.eventbus.EventBus eventBus) {
		log.info("Autoconfigured: IAdhocEventBus over {}", eventBus.getClass().getName());
		return AdhocEventBusHelpersUnsafe.safeWrapper(eventBus::post);
	}

	/**
	 * In-memory plan registry, capped by total live node count. Pivotable's UI Live View polls this to render an "what
	 * is the query doing right now" indicator. The default cap of 200k nodes is large enough to retain a handful of
	 * 20k-step plans simultaneously without unbounded growth.
	 *
	 * <p>
	 * <strong>Singleton-shared bean.</strong> The cube engine writes plan fragments here while the Pivotable HTTP plan
	 * handlers read from it; both consumers MUST receive the same instance. Users may override this bean (e.g. with a
	 * {@code NoopQueryPlanRegistry} for production setups that don't need the feature), but if they ALSO override a
	 * downstream bean such as {@link IQueryPreparator}, {@link ICubeQueryEngine}, or {@code AdhocSchema}, they must
	 * inject this bean into the override rather than constructing a fresh registry inline. Otherwise the engine writes
	 * to one instance, the handlers read from another, and the plan endpoints silently return empty. See
	 * {@code docs/pivotable-spring-beans.md} for the full list of singleton-sharing beans.
	 *
	 * @return a {@link BoundedQueryPlanRegistry}; users can override with their own bean (e.g. a no-op for production
	 *         setups that don't need the feature).
	 */
	@Bean
	@ConditionalOnMissingBean(IQueryPlanRegistry.class)
	public IQueryPlanRegistry queryPlanRegistry(Environment env) {
		long size = env
				.getProperty("adhoc.pivotable.queryPlanRegistry.size", Long.class, DEFAULT_PLAN_REGISTRY_NODE_BUDGET);
		return new BoundedQueryPlanRegistry(size);
	}

	@Bean
	@ConditionalOnMissingBean(IImplicitOptions.class)
	public IImplicitOptions implicitOptions(Environment env) {
		return SpringImplicitOptions.builder().env(env).build();
	}

	@Bean
	@ConditionalOnMissingBean(IImplicitFilter.class)
	public IImplicitFilter implicitFilter() {
		return _ -> ISliceFilter.MATCH_ALL;
	}

	@Bean
	@ConditionalOnMissingBean(IQueryStepCache.class)
	public IQueryStepCache queryStepCache(Environment env) {
		long size = env.getProperty("adhoc.pivotable.queryStepCache.size", Long.class, DEFAULT_QUERY_STEP_CACHE_BUDGET);
		return GuavaQueryStepCache.withSize(size);
	}

	@Bean
	@ConditionalOnMissingBean(IQueryPreparator.class)
	public IQueryPreparator queryPreparator(IAdhocFactories factories,
			IImplicitOptions implicitOptions,
			IImplicitFilter implicitFilter,
			IQueryStepCache queryStepCache,
			IQueryPlanRegistry queryPlanRegistry) {
		return StandardQueryPreparator.builder()
				.factories(factories)
				.implicitOptions(implicitOptions)
				.implicitFilter(implicitFilter)
				.queryStepCache(queryStepCache)
				.queryPlanRegistry(queryPlanRegistry)
				.build();
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
