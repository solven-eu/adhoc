package eu.solven.adhoc.pivotable.spring;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.util.IAdhocEventBus;
import eu.solven.adhoc.util.IStopwatchFactory;

/**
 * The beans needed to instantiate Adhoc components
 * 
 * @author Benoit Lacelle
 */
@AutoConfiguration
public class AdhocAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean(value = { IAdhocEventBus.class,
			com.google.common.eventbus.EventBus.class,
			org.greenrobot.eventbus.EventBus.class })
	public IAdhocEventBus adhocEventBus() {
		return new com.google.common.eventbus.EventBus("Adhoc")::post;
	}

	@Bean
	@ConditionalOnMissingBean(value = IAdhocEventBus.class)
	@ConditionalOnBean(value = com.google.common.eventbus.EventBus.class)
	public IAdhocEventBus adhocEventBusFromGuava(com.google.common.eventbus.EventBus eventBus) {
		return eventBus::post;
	}

	@Bean
	@ConditionalOnMissingBean(value = IAdhocEventBus.class)
	@ConditionalOnBean(value = org.greenrobot.eventbus.EventBus.class)
	public IAdhocEventBus adhocEventBusFromGreenRobot(org.greenrobot.eventbus.EventBus eventBus) {
		return eventBus::post;
	}

	@Bean
	@ConditionalOnMissingBean(value = IOperatorsFactory.class)
	public IOperatorsFactory adhocOperatorsFactory() {
		return new StandardOperatorsFactory();
	}

	@Bean
	@ConditionalOnMissingBean(value = IStopwatchFactory.class)
	public IStopwatchFactory stopwatchFactory() {
		return IStopwatchFactory.guavaStopwatchFactory();
	}

	@Bean
	@ConditionalOnMissingBean(value = IAdhocQueryEngine.class)
	public IAdhocQueryEngine adhocQueryEngine(IAdhocEventBus eventBus,
			IOperatorsFactory operatorsFactory,
			IStopwatchFactory stopwatchFactory) {
		return AdhocQueryEngine.builder()
				.eventBus(eventBus)
				.operatorsFactory(operatorsFactory)
				.stopwatchFactory(stopwatchFactory)
				.build();
	}
}
