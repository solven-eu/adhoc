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
