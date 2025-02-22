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
package eu.solven.adhoc.pivotable.webflux.api;

import static org.springdoc.core.fn.builders.apiresponse.Builder.responseBuilder;

import java.util.Map;

import org.springdoc.webflux.core.fn.SpringdocRouteBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointMetadata;
import eu.solven.adhoc.pivotable.entrypoint.PivotableEntrypointsHandler;
import eu.solven.adhoc.pivotable.query.PivotableQueryHandler;
import eu.solven.adhoc.storage.ITabularView;
import lombok.extern.slf4j.Slf4j;

/**
 * Redirect each route (e.g. `/games/someGameId`) to the appropriate handler.
 *
 * @author Benoit Lacelle
 *
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
public class PivotableApiRouter {

	private static final RequestPredicate json(String path) {
		final RequestPredicate json = RequestPredicates.accept(MediaType.APPLICATION_JSON);
		return RequestPredicates.path("/api/v1" + path).and(json);
	}

	// https://github.com/springdoc/springdoc-openapi-demos/tree/2.x/springdoc-openapi-spring-boot-2-webflux-functional
	// https://stackoverflow.com/questions/6845772/should-i-use-singular-or-plural-name-convention-for-rest-resources
	@Bean
	public RouterFunction<ServerResponse> apiRoutes(PivotableEntrypointsHandler entrypointsHandler,
			PivotableQueryHandler queryHandler) {

		// Builder accountId = parameterBuilder().name("account_id").description("Search for a specific accountId");

		return SpringdocRouteBuilder.route()
				.GET(json("/entrypoints"),
						entrypointsHandler::listEntrypoints,
						ops -> ops.operationId("loadEntrypoints")
								// .parameter(gameId)
								.response(responseBuilder().responseCode("200")
										.implementationArray(AdhocEntrypointMetadata.class)))

				.GET(json("/cubes/metadata"),
						queryHandler::loadMetadata,
						ops -> ops.operationId("loadMetadata")
								// .parameter(gameId)
								.response(responseBuilder().responseCode("200").implementation(Map.class)))

				// simple AdhocQuery with a GET
				.GET(json("/cubes/execute"),
						queryHandler::executeFlatQuery,
						ops -> ops.operationId("executeQuery")
								.response(responseBuilder().responseCode("200").implementation(ITabularView.class)))

				// complex AdhocQuery with a POST
				.POST(json("/cubes/execute"),
						queryHandler::executeQuery,
						ops -> ops.operationId("executeQuery")
								.response(responseBuilder().responseCode("200").implementation(ITabularView.class)))

				.build();

	}
}