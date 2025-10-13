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
package eu.solven.adhoc.pivotable.query;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.data.tabular.ListBasedTabularView;
import eu.solven.adhoc.pivotable.cube.AdhocCubesRegistry;
import eu.solven.adhoc.pivotable.cube.PivotableCubeId;
import eu.solven.adhoc.pivotable.cube.PivotableCubeMetadata;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocSchemaRegistry;
import eu.solven.adhoc.pivotable.query.PivotableAsynchronousQueriesManager.StateAndView;
import eu.solven.adhoc.pivotable.webflux.api.AdhocHandlerHelper;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Pivotable API for queries.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PivotableQueryHandler {
	final PivotableAdhocSchemaRegistry schemaRegistry;
	final AdhocCubesRegistry cubesRegistry;

	final PivotableAsynchronousQueriesManager asynchronousQueriesManager = new PivotableAsynchronousQueriesManager();

	public Mono<ServerResponse> loadCubeSchema(ServerRequest serverRequest) {
		UUID endpointId = AdhocHandlerHelper.uuid(serverRequest, "endpoint_id");
		String cubeName = AdhocHandlerHelper.string(serverRequest, "cube");

		PivotableCubeMetadata cube = cubesRegistry.getCube(PivotableCubeId.of(endpointId, cubeName));

		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(cube));
	}

	protected Mono<ServerResponse> executeQuery(Mono<TargetedCubeQuery> queryOnSchemaMono) {
		return queryOnSchemaMono.map(queryOnSchema -> {
			AdhocSchema schema = schemaRegistry.getSchema(queryOnSchema.getEndpointId());

			return schema.execute(queryOnSchema.getCube(), queryOnSchema.getQuery());
		})
				// ListBasedTabularView is serializable with Jackson
				.map(ListBasedTabularView::load)
				.flatMap(view -> ServerResponse.ok()
						.contentType(MediaType.APPLICATION_JSON)
						.body(BodyInserters.fromValue(view)));
	}

	protected Mono<ServerResponse> executeAsynchronousQuery(Mono<TargetedCubeQuery> queryOnSchemaMono) {
		return queryOnSchemaMono.map(queryOnSchema -> {
			AdhocSchema schema = schemaRegistry.getSchema(queryOnSchema.getEndpointId());

			return asynchronousQueriesManager.execute(schema, queryOnSchema);

		})
				.flatMap(view -> ServerResponse.ok()
						.contentType(MediaType.APPLICATION_JSON)
						.body(BodyInserters.fromValue(view)));
	}

	/**
	 * Execute an {@link eu.solven.adhoc.query.cube.IAdhocQuery} defined through POST parameter.
	 *
	 * @param serverRequest
	 * @return
	 */
	public Mono<ServerResponse> executeQuery(ServerRequest serverRequest) {
		Mono<TargetedCubeQuery> queryOnSchemaMono = serverRequest.bodyToMono(TargetedCubeQuery.class);
		return executeQuery(queryOnSchemaMono);
	}

	/**
	 * Execute an {@link eu.solven.adhoc.query.cube.IAdhocQuery} defined through POST parameter.
	 *
	 * @param serverRequest
	 * @return
	 */
	public Mono<ServerResponse> executeAsynchronousQuery(ServerRequest serverRequest) {
		Mono<TargetedCubeQuery> queryOnSchemaMono = serverRequest.bodyToMono(TargetedCubeQuery.class);
		return executeAsynchronousQuery(queryOnSchemaMono);
	}

	public Mono<ServerResponse> fetchQueryResult(ServerRequest serverRequest) {
		UUID queryId = AdhocHandlerHelper.uuid(serverRequest, "query_id");
		boolean withView = AdhocHandlerHelper.optBoolean(serverRequest, "with_view").orElse(true);

		StateAndView optView = asynchronousQueriesManager.getStateAndView(queryId);

		if (withView && optView.getOptView().isPresent()) {

			// ListBasedTabularView is serializable with Jackson
			ListBasedTabularView view = ListBasedTabularView.load(optView.getOptView().get());

			QueryResultHolder body = QueryResultHolder.served(optView.getState(), view);
			return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(body));
		} else {
			AsynchronousStatus state = asynchronousQueriesManager.getState(queryId);

			Optional<Duration> optRetryIn = getRetryIn(state);

			if (optRetryIn.isPresent()) {
				QueryResultHolder body = QueryResultHolder.retry(state, optRetryIn.get());
				return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(body));
			} else {
				QueryResultHolder body = QueryResultHolder.discarded(state);
				return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(body));
			}
		}
	}

	protected Optional<Duration> getRetryIn(AsynchronousStatus state) {
		return switch (state) {
		case AsynchronousStatus.DISCARDED:
		case AsynchronousStatus.UNKNOWN:
		case AsynchronousStatus.FAILED: {
			yield Optional.empty();
		}
		case AsynchronousStatus.SERVED: {
			yield Optional.of(Duration.ofSeconds(0));
		}
		case AsynchronousStatus.RUNNING: {
			// TODO Introduce exponential back-off
			yield Optional.of(Duration.ofSeconds(1));
		}
		};
	}

	/**
	 * Execute an {@link eu.solven.adhoc.query.cube.IAdhocQuery} defined through GET parameters.
	 *
	 * @param serverRequest
	 * @return
	 */
	// cube=someCube&measure=k1.SUM&column_a=a1,a2&column_b>123&group_by=c
	public Mono<ServerResponse> executeFlatQuery(ServerRequest serverRequest) {
		TargetedCubeQuery.TargetedCubeQueryBuilder queryOnSchemaBuilder = TargetedCubeQuery.builder();

		String cubeName = AdhocHandlerHelper.string(serverRequest, "cube_name");
		queryOnSchemaBuilder.cube(cubeName);

		CubeQuery.CubeQueryBuilder queryBuilder = CubeQuery.builder();

		// https://stackoverflow.com/questions/24059773/correct-way-to-pass-multiple-values-for-same-parameter-name-in-get-request
		// `tag=a&tag=b` means we are looking for `a AND b`
		// `tag=a,b` means we are looking for `a OR b`
		// `tag=a,b&tag=c` means we are looking for `(a AND b) OR c`
		List<String> measures = serverRequest.queryParams().get("measure");
		if (measures != null) {
			queryBuilder.measureNames(measures);
		}
		List<String> groupBys = serverRequest.queryParams().get("group_by");
		if (measures != null) {
			groupBys.forEach(queryBuilder::groupByAlso);
		}

		{
			List<String> filteredColumns = serverRequest.queryParams()
					.keySet()
					.stream()
					.filter(k -> k.startsWith("column_"))
					.map(k -> k.substring("column_".length()))
					.distinct()
					.toList();

			// This simpler API do AND between input filteredColumns
			AndFilter.AndFilterBuilder andBuilder = AndFilter.builder();

			filteredColumns.forEach(filteredColumn -> {
				List<String> rawFilters = serverRequest.queryParams().get("column_" + filteredColumn);

				List<IValueMatcher> filters = rawFilters.stream()
						.distinct()
						// This equalsMatcher may later be turned into a StringMatcher
						.<IValueMatcher>map(EqualsMatcher::matchEq)
						.toList();

				ColumnFilter columnFilter = ColumnFilter.builder()
						.column(filteredColumn)
						.valueMatcher(OrMatcher.builder().operands(filters).build())
						.build();
				andBuilder.and(columnFilter);
			});
			queryBuilder.filter(andBuilder.build());
		}
		queryOnSchemaBuilder.query(queryBuilder.build());

		return executeQuery(Mono.just(queryOnSchemaBuilder.build()));
	}
}