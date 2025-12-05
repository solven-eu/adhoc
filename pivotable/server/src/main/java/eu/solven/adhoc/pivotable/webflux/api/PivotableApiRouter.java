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

import java.util.UUID;

import org.springdoc.webflux.core.fn.SpringdocRouteBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.beta.schema.ColumnStatistics;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.data.tabular.ListBasedTabularView;
import eu.solven.adhoc.pivotable.cube.PivotableCubeMetadata;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.PivotableEndpointsHandler;
import eu.solven.adhoc.pivotable.endpoint.TargetedEndpointSchemaMetadata;
import eu.solven.adhoc.pivotable.query.PivotableQueryHandler;
import eu.solven.adhoc.pivotable.query.QueryResultHolder;
import eu.solven.adhoc.pivottable.api.IPivotableApiConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * Redirect each route (e.g. `/games/someGameId`) to the appropriate handler.
 *
 * @author Benoit Lacelle
 *
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class PivotableApiRouter {

	private static RequestPredicate json(String path) {
		final RequestPredicate json = RequestPredicates.accept(MediaType.APPLICATION_JSON);
		return RequestPredicates.path(IPivotableApiConstants.PREFIX + path).and(json);
	}

	/**
	 * Register the routes of the standard Pivotable API, dicovering the schema and executing queries.
	 * 
	 * @param endpointsHandler
	 * @param queryHandler
	 * @return
	 */
	// https://github.com/springdoc/springdoc-openapi-demos/tree/2.x/springdoc-openapi-spring-boot-2-webflux-functional
	// https://stackoverflow.com/questions/6845772/should-i-use-singular-or-plural-name-convention-for-rest-resources
	@Bean
	public RouterFunction<ServerResponse> apiRoutes(PivotableEndpointsHandler endpointsHandler,
			PivotableQueryHandler queryHandler) {

		var endpointId = parameterBuilder().name("endpoint_id")
				.description("Search for a specific endpointId")
				.example("12345678-1234-1234-1234-123456789012");

		var cubeId = parameterBuilder().name("cube")
				.description("Search for a specific cube by its name")
				.example("someCubeName");

		var table =
				parameterBuilder().name("table").description("Search for a specific table").example("somreCubeName");

		var name = parameterBuilder().name("name")
				.description("A specific name, for the main requested type")
				.example("someCubeName");

		var coordinate = parameterBuilder().name("coordinate")
				.description("Search for a specific coordinate, along one or more column")
				.example("someCoordinate");

		var coordinatesLimit = parameterBuilder().name("limit_coordinates")
				.description(
						"Maximum number of coordinates to return per column. -1 for no limit. Missing will fallback on a reasonnable default.")
				.example("100")
				.implementation(Integer.class);

		return SpringdocRouteBuilder.route()
				.GET(json("/endpoints"),
						endpointsHandler::listEntrypoints,
						ops -> ops.operationId("loadEntrypoints")
								.parameter(endpointId)
								.response(responseBuilder().responseCode("200")
										.implementationArray(PivotableAdhocEndpointMetadata.class)))
				.GET(json("/endpoints/schemas"),
						endpointsHandler::endpointSchema,
						ops -> ops.operationId("loadMetadata")
								.parameter(endpointId)
								.response(responseBuilder().responseCode("200")
										.implementationArray(TargetedEndpointSchemaMetadata.class)))

				.GET(json("/endpoints/schemas/columns"),
						endpointsHandler::searchColumns,
						ops -> ops.operationId("listColumns")
								.parameter(endpointId)
								.parameter(cubeId)
								.parameter(table)
								.parameter(name)
								.parameter(coordinate)
								.parameter(coordinatesLimit)
								.response(responseBuilder().responseCode("200")
										.implementationArray(ColumnStatistics.class)))

				.GET(json("/cubes/schemas"),
						queryHandler::loadCubeSchema,
						ops -> ops.operationId("loadMetadata")
								.parameter(endpointId.required(true))
								.parameter(cubeId.required(true))
								.response(responseBuilder().responseCode("200")
										.implementationArray(PivotableCubeMetadata.class)))

				// simple AdhocQuery with a GET
				.GET(json("/cubes/query"),
						queryHandler::executeFlatQuery,
						ops -> ops.operationId("executeQuery")
								// the targeted cube can be referred through id or name
								.parameter(endpointId)
								.parameter(cubeId)

								.parameter(parameterBuilder().name("measure")
										.description("Some pre-aggregated or transformed measure")
										.example("delta.EUR"))
								.parameter(parameterBuilder().name("group_by")
										.description("Columns which are decomposed")
										.example("ccy,country"))
								.parameter(parameterBuilder().name("column_color")
										.description("A filter expression for column `color`")
										.example("red"))

								.parameter(parameterBuilder().name("query_option")
										.description("A set of options (e.g. StandardQueryOptions)")
										.example("red"))

								.response(responseBuilder().responseCode("200")
										.implementation(ListBasedTabularView.class)))

				// complex AdhocQuery with a POST
				.POST(json("/cubes/query"),
						queryHandler::executeQuery,
						ops -> ops.operationId("executeQuery")
								// .parameter(parameterBuilder().name("synchronous")
								// .description("If false, returns a resultId to be polled")
								// .implementation(Boolean.class)
								// .example("true")
								// .example("false"))
								.requestBody(org.springdoc.core.fn.builders.requestbody.Builder.requestBodyBuilder()
										.implementation(TargetedCubeQuery.class))
								.response(responseBuilder().responseCode("200")
										.implementation(ListBasedTabularView.class)))
				.POST(json("/cubes/query/asynchronous"),
						queryHandler::executeAsynchronousQuery,
						ops -> ops.operationId("executeQuery")
								.requestBody(org.springdoc.core.fn.builders.requestbody.Builder.requestBodyBuilder()
										.implementation(TargetedCubeQuery.class))
								.response(responseBuilder().responseCode("200").implementation(UUID.class)))
				.DELETE(json("/cubes/query"),
						queryHandler::cancelQuery,
						ops -> ops.operationId("cancelQuery")
								.parameter(parameterBuilder().name("query_id")
										.description("id of the query result")
										.implementation(UUID.class)
										.example("12345678-1234-1234-1234-123456789012"))
								.response(responseBuilder().responseCode("200").implementation(String.class)))

				.GET(json("/cubes/query/result"),
						queryHandler::fetchQueryResult,
						ops -> ops.operationId("fetchQueryResult")
								.parameter(parameterBuilder().name("result_id")
										.description("id of the query result")
										.implementation(UUID.class)
										.example("12345678-1234-1234-1234-123456789012"))
								.parameter(parameterBuilder().name("with_view")
										.description("Should the view be returned if state is SERVED? Default is true")
										.implementation(Boolean.class)
										.example("true")
										.example("false"))
								.response(
										responseBuilder().responseCode("200").implementation(QueryResultHolder.class)))

				.build();

	}
}