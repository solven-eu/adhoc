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

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.IColumnFactory;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.engine.StandardColumnFactory;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.map.ISliceFactory;
import eu.solven.adhoc.map.StandardSliceFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.util.IStopwatchFactory;

/**
 * The beans needed to instantiate Adhoc components
 * 
 * @author Benoit Lacelle
 */
@AutoConfiguration
public class AdhocAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean({ IAdhocEventBus.class,
			com.google.common.eventbus.EventBus.class,
			org.greenrobot.eventbus.EventBus.class })
	public IAdhocEventBus adhocEventBus() {
		return new com.google.common.eventbus.EventBus("Adhoc")::post;
	}

	@Bean
	@ConditionalOnMissingBean(IAdhocEventBus.class)
	@ConditionalOnBean(com.google.common.eventbus.EventBus.class)
	public IAdhocEventBus adhocEventBusFromGuava(com.google.common.eventbus.EventBus eventBus) {
		return eventBus::post;
	}

	@Bean
	@ConditionalOnMissingBean(IAdhocEventBus.class)
	@ConditionalOnBean(org.greenrobot.eventbus.EventBus.class)
	public IAdhocEventBus adhocEventBusFromGreenRobot(org.greenrobot.eventbus.EventBus eventBus) {
		return eventBus::post;
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

	@Bean
	@ConditionalOnMissingBean(ISliceFactory.class)
	public ISliceFactory sliceFactory() {
		return StandardSliceFactory.builder().build();
	}

	@Bean
	@ConditionalOnMissingBean(IStopwatchFactory.class)
	public IStopwatchFactory stopwatchFactory() {
		return IStopwatchFactory.guavaStopwatchFactory();
	}

	@Bean
	@ConditionalOnMissingBean(AdhocFactories.class)
	public AdhocFactories adhocFactories(IOperatorFactory operatorFactory,
			IColumnFactory columnsFactory,
			ISliceFactory sliceFactory,
			IStopwatchFactory stopwatchFactory) {
		return AdhocFactories.builder()
				.operatorFactory(operatorFactory)
				.columnFactory(columnsFactory)
				.stopwatchFactory(stopwatchFactory)
				.sliceFactory(sliceFactory)
				.build();
	}

	@Bean
	@ConditionalOnMissingBean(ICubeQueryEngine.class)
	public ICubeQueryEngine adhocQueryEngine(IAdhocEventBus eventBus, AdhocFactories adhocFactories) {
		return CubeQueryEngine.builder().eventBus(eventBus).factories(adhocFactories).build();
	}
}
