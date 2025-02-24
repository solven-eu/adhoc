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
import static org.springdoc.core.fn.builders.parameter.Builder.parameterBuilder;

import java.util.Map;

import org.springdoc.webflux.core.fn.SpringdocRouteBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.beta.schema.TargetedAdhocQuery;
import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointMetadata;
import eu.solven.adhoc.pivotable.entrypoint.EntrypointSchema;
import eu.solven.adhoc.pivotable.entrypoint.PivotableEntrypointsHandler;
import eu.solven.adhoc.pivotable.query.PivotableQueryHandler;
import eu.solven.adhoc.storage.ListBasedTabularView;
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

		var entrypointId = parameterBuilder().name("entrypoint_id")
				.description("Search for a specific entrypointId")
				.example("12345678-1234-1234-1234-123456789012");

		var cubeId = parameterBuilder().name("cube_id")
				.description("Search for a specific cubeId")
				.example("12345678-1234-1234-1234-123456789012");

		var cubeName = parameterBuilder().name("cube_name")
				.description("Search for a specific cubeName")
				.example("someCubeName");

		return SpringdocRouteBuilder.route()
				.GET(json("/entrypoints"),
						entrypointsHandler::listEntrypoints,
						ops -> ops.operationId("loadEntrypoints")
								.parameter(entrypointId)
								.response(responseBuilder().responseCode("200")
										.implementationArray(AdhocEntrypointMetadata.class)))
				.GET(json("/entrypoints/schemas"),
						entrypointsHandler::entrypointSchema,
						ops -> ops.operationId("loadMetadata")
								.parameter(entrypointId)
								.response(responseBuilder().responseCode("200")
										.implementationArray(EntrypointSchema.class)))

				.GET(json("/cubes/metadata"),
						queryHandler::loadMetadata,
						ops -> ops.operationId("loadMetadata")
								// .parameter(gameId)
								.response(responseBuilder().responseCode("200").implementation(Map.class)))

				// simple AdhocQuery with a GET
				.GET(json("/cubes/query"),
						queryHandler::executeFlatQuery,
						ops -> ops.operationId("executeQuery")
								// the targeted cube can be referred through id or name
								.parameter(cubeId)
								.parameter(cubeName)

								.parameter(parameterBuilder().name("measure")
										.description("Some pre-aggregated or transformed measure")
										.example("delta.EUR"))
								.parameter(parameterBuilder().name("group_by")
										.description("Columns which are decomposed")
										.example("ccy,country"))
								.parameter(parameterBuilder().name("column_color")
										.description("A filter expression for column `color`")
										.example("red"))
								.response(responseBuilder().responseCode("200")
										.implementation(ListBasedTabularView.class)))

				// complex AdhocQuery with a POST
				.POST(json("/cubes/query"),
						queryHandler::executeQuery,
						ops -> ops.operationId("executeQuery")
								.requestBody(org.springdoc.core.fn.builders.requestbody.Builder.requestBodyBuilder()
										.implementation(TargetedAdhocQuery.class))
								.response(responseBuilder().responseCode("200")
										.implementation(ListBasedTabularView.class)))

				.build();

	}
}