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
package eu.solven.adhoc.pivotable.core;

import org.greenrobot.eventbus.EventBus;
import org.greenrobot.eventbus.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import eu.solven.adhoc.beta.schema.AdhocSchemaForApi;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.app.InjectPivotableAccountsConfig;
import eu.solven.adhoc.pivotable.app.InjectPivotableEntrypointsConfig;
import eu.solven.adhoc.pivotable.app.InjectSimpleCubesConfig;
import eu.solven.adhoc.pivotable.app.persistence.InMemoryPivotableConfiguration;
import eu.solven.adhoc.pivotable.cube.AdhocCubesRegistry;
import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointsRegistry;
import eu.solven.adhoc.pivotable.eventbus.EventBusLogger;
import eu.solven.adhoc.tools.PivotableRandomConfiguration;
import lombok.extern.slf4j.Slf4j;

@Configuration
@Import({

		PivotableRandomConfiguration.class,

		PivotableUsersRegistry.class,
		AdhocEntrypointsRegistry.class,
		AdhocCubesRegistry.class,

		// Only one of the following persistence options will actually kicks-in
		InMemoryPivotableConfiguration.class,

		InjectPivotableAccountsConfig.class,
		InjectPivotableEntrypointsConfig.class,
		InjectSimpleCubesConfig.class,

})
@Slf4j
public class PivotableComponentsConfiguration {

	private Logger makeLogger() {
		return new EventBusLogger();
	}

	@Bean
	public EventBus eventBus() {
		EventBus eventBus = EventBus.builder()
				.strictMethodVerification(true)
				.throwSubscriberException(true)
				.logger(makeLogger())
				.build();

		eventBus.register(new EventBusLogger());

		return eventBus;
	}

	@Bean
	public Void registerAdhocLog(EventBus eventBus) {
		eventBus.register(new AdhocEventFromGreenrobotToSlf4j());

		return null;
	}

	// A custom project may prepare a schema with relevant tables, measures, etc
	@Bean
	AdhocSchemaForApi adhocSchemaForApi(EventBus eventBus) {
		return AdhocSchemaForApi.builder().engine(AdhocQueryEngine.builder().eventBus(eventBus::post).build()).build();
	}
}
