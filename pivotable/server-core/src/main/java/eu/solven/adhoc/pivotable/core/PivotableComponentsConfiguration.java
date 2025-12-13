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
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.app.InjectPivotableAccountsConfig;
import eu.solven.adhoc.pivotable.app.InjectPivotableSelfEndpointConfig;
import eu.solven.adhoc.pivotable.app.persistence.InMemoryPivotableConfiguration;
import eu.solven.adhoc.pivotable.cube.AdhocCubesRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocSchemaRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableEndpointsRegistry;
import eu.solven.adhoc.pivotable.eventbus.EventBusLogger;
import eu.solven.adhoc.tools.PivotableRandomConfiguration;
import eu.solven.adhoc.util.ThrowableAsStackSerializer;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.module.SimpleModule;

/**
 * Spring {@link Configuration} for Pivotable components.
 * 
 * @author Benoit Lacelle
 */
@Configuration
@Import({

		PivotableRandomConfiguration.class,

		PivotableUsersRegistry.class,
		// Holds the Entrypoints definitions (e.g. URLs)
		PivotableEndpointsRegistry.class,
		// Holds the Entrypoints schemas
		PivotableAdhocSchemaRegistry.class,
		// Holds the Cubes, through endpoints
		AdhocCubesRegistry.class,

		// Only one of the following persistence options will actually kicks-in
		InMemoryPivotableConfiguration.class,

		// Adhoc specific beans, like the engine
		// AdhocAutoConfiguration.class,

		InjectPivotableAccountsConfig.class,
		InjectPivotableSelfEndpointConfig.class,

})
@Slf4j
public class PivotableComponentsConfiguration {

	private Logger makeLogger() {
		return new EventBusLogger();
	}

	// BEWARE Is it legit for Pivotable to prefer Greenrobot?
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
		AdhocEventFromGreenrobotToSlf4j subscriber = new AdhocEventFromGreenrobotToSlf4j();
		log.info("Registering {} in {}", subscriber, eventBus);
		eventBus.register(subscriber);

		return null;
	}

	@Bean
	public JsonMapperBuilderCustomizer jsonCustomizer() {
		// https://www.baeldung.com/spring-boot-customize-jackson-objectmapper
		return builder -> {
			SimpleModule module = new SimpleModule();
			module.addSerializer(new ThrowableAsStackSerializer());
			
			builder.addserializerFactory().withAdditionalKeySerializers();
			// AdhocJackson.indentArrayWithEol(builder);
		};
	}
}
