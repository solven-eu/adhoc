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
package eu.solven.adhoc.pivotable.app;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocSchemaRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableEndpointsRegistry;
import lombok.extern.slf4j.Slf4j;

/**
 * Register `http://localhost:self` as an endpoint, so that Pivotable server is also a Adhoc server.
 * 
 * @author Benoit Lacelle
 */
@Configuration
@Slf4j
public class InjectPivotableSelfEndpointConfig {

	@Profile(IPivotableSpringProfiles.P_SELF_ENDPOINT)
	@Qualifier(IPivotableSpringProfiles.P_SELF_ENDPOINT)
	@Bean
	public PivotableAdhocEndpointMetadata initSelfEntrypoint(PivotableEndpointsRegistry endpointsRegistry) {
		log.info("Registering the {} endpoint", IPivotableSpringProfiles.P_SELF_ENDPOINT);

		PivotableAdhocEndpointMetadata self = PivotableAdhocEndpointMetadata.localhost();
		endpointsRegistry.registerEntrypoint(self);

		return self;
	}

	@Profile(IPivotableSpringProfiles.P_SELF_ENDPOINT)
	@Qualifier(IPivotableSpringProfiles.P_SELF_ENDPOINT)
	@Bean
	public AdhocSchema registerSelfSchema(IAdhocQueryEngine engine, PivotableAdhocSchemaRegistry schemaRegistry) {
		AdhocSchema selfSchema = AdhocSchema.builder().engine(engine).build();

		PivotableAdhocEndpointMetadata self = PivotableAdhocEndpointMetadata.localhost();
		schemaRegistry.registerEntrypoint(self.getId(), selfSchema);

		log.info("Registering the {} schema", IPivotableSpringProfiles.P_SELF_ENDPOINT);
		return selfSchema;
	}
}
