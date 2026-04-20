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
package eu.solven.adhoc.pivotable.webmvc.api;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.dataframe.tabular.IReadableTabularView;
import eu.solven.adhoc.dataframe.tabular.ITabularViewArrowSerializer;
import eu.solven.adhoc.dataframe.tabular.ListBasedTabularView;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.OrMatcher;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.cube.AdhocCubesRegistry;
import eu.solven.adhoc.pivotable.cube.PivotableCubeId;
import eu.solven.adhoc.pivotable.cube.PivotableCubeMetadata;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import eu.solven.adhoc.pivotable.query.AsynchronousStatus;
import eu.solven.adhoc.pivotable.query.CancellationStatus;
import eu.solven.adhoc.pivotable.query.PivotableAsynchronousQueriesManager;
import eu.solven.adhoc.pivotable.query.PivotableAsynchronousQueriesManager.StateAndView;
import eu.solven.adhoc.pivotable.query.QueryResultHolder;
import eu.solven.adhoc.pivotable.webnone.api.IPivotableRouteConstants;
import eu.solven.adhoc.query.cube.CubeQuery;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Pivotable API for executing queries and managing asynchronous query results.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
@RestController
@RequestMapping(IPivotableApiConstants.PREFIX)
public class PivotableQueryController implements IPivotableRouteConstants {

	public static final MediaType ARROW_STREAM_MEDIA_TYPE =
			MediaType.parseMediaType("application/vnd.apache.arrow.stream");

	final PivotableSchemaRegistry schemaRegistry;
	final AdhocCubesRegistry cubesRegistry;

	final PivotableAsynchronousQueriesManager asynchronousQueriesManager = new PivotableAsynchronousQueriesManager();

	final AtomicLongMap<UUID> queryIdPolls = AtomicLongMap.create();

	/**
	 * @param endpointId
	 *            UUID of the endpoint
	 * @param cube
	 *            name of the cube
	 * @return cube metadata
	 */
	@GetMapping(value = "/cubes/schemas", produces = MediaType.APPLICATION_JSON_VALUE)
	public PivotableCubeMetadata loadCubeSchema(@RequestParam("endpoint_id") String endpointId,
			@RequestParam String cube) {
		UUID endpointUuid = UUID.fromString(endpointId);
		return cubesRegistry.getCube(PivotableCubeId.of(endpointUuid, cube));
	}

	/**
	 * Execute a query expressed via GET parameters.
	 *
	 * @param request
	 *            the current HTTP request carrying query parameters
	 * @return query result as a {@link ListBasedTabularView}
	 */
	// cube=someCube&measure=k1.SUM&column_a=a1,a2&column_b>123&group_by=c
	@GetMapping(value = R_CUBE_QUERY, produces = MediaType.APPLICATION_JSON_VALUE)
	public ListBasedTabularView executeFlatQuery(HttpServletRequest request) {
		TargetedCubeQuery.TargetedCubeQueryBuilder queryOnSchemaBuilder = TargetedCubeQuery.builder();

		String cubeName = request.getParameter("cube_name");
		if (cubeName != null) {
			queryOnSchemaBuilder.cube(cubeName);
		}

		CubeQuery.CubeQueryBuilder queryBuilder = CubeQuery.builder();

		String[] measures = request.getParameterValues("measure");
		if (measures != null) {
			queryBuilder.measureNames(ImmutableList.copyOf(measures));
		}
		String[] groupBys = request.getParameterValues("group_by");
		if (groupBys != null) {
			for (String groupBy : groupBys) {
				queryBuilder.groupByAlso(groupBy);
			}
		}

		{
			List<String> filteredColumns = request.getParameterMap()
					.keySet()
					.stream()
					.filter(k -> k.startsWith("column_"))
					.map(k -> k.substring("column_".length()))
					.distinct()
					.toList();

			AndFilter.AndFilterBuilder andBuilder = AndFilter.builder();

			filteredColumns.forEach(filteredColumn -> {
				String[] rawFilters = request.getParameterValues("column_" + filteredColumn);

				List<IValueMatcher> filters =
						List.of(rawFilters).stream().distinct().<IValueMatcher>map(EqualsMatcher::matchEq).toList();

				ColumnFilter columnFilter =
						ColumnFilter.builder().column(filteredColumn).valueMatcher(OrMatcher.copyOf(filters)).build();
				andBuilder.and(columnFilter);
			});
			queryBuilder.filter(andBuilder.build());
		}
		queryOnSchemaBuilder.query(queryBuilder.build());

		return executeQuery(queryOnSchemaBuilder.build());
	}

	/**
	 * Execute a query expressed as a JSON body.
	 *
	 * @param query
	 *            the targeted cube query
	 * @return query result as a {@link ListBasedTabularView}
	 */
	@PostMapping(value = R_CUBE_QUERY, produces = MediaType.APPLICATION_JSON_VALUE)
	public ListBasedTabularView executeQuery(@RequestBody TargetedCubeQuery query) {
		IAdhocSchema schema = schemaRegistry.getSchema(query.getEndpointId());
		IReadableTabularView view = schema.execute(query.getCube(), query.getQuery());
		// ListBasedTabularView is serializable with Jackson
		return ListBasedTabularView.load(view);
	}

	/**
	 * Execute a query and return the result as an Apache Arrow IPC stream. Requires an
	 * {@link ITabularViewArrowSerializer} on the classpath (e.g. {@code adhoc-experimental}).
	 *
	 * @param query
	 *            the targeted cube query
	 * @return streaming Arrow IPC response
	 */
	@PostMapping(value = R_CUBE_QUERY, produces = "application/vnd.apache.arrow.stream")
	public ResponseEntity<StreamingResponseBody> executeQueryAsArrow(@RequestBody TargetedCubeQuery query) {
		IAdhocSchema schema = schemaRegistry.getSchema(query.getEndpointId());
		IReadableTabularView view = schema.execute(query.getCube(), query.getQuery());

		StreamingResponseBody stream = outputStream -> serializeToArrow(view, outputStream);

		return ResponseEntity.ok().contentType(ARROW_STREAM_MEDIA_TYPE).body(stream);
	}

	/**
	 * Submit a query for asynchronous execution.
	 *
	 * @param query
	 *            the targeted cube query
	 * @return the UUID of the pending query result
	 */
	@PostMapping(value = "/cubes/query/asynchronous", produces = MediaType.APPLICATION_JSON_VALUE)
	public UUID executeAsynchronousQuery(@RequestBody TargetedCubeQuery query) {
		IAdhocSchema schema = schemaRegistry.getSchema(query.getEndpointId());
		return asynchronousQueriesManager.executeAsync(schema, query);
	}

	/**
	 * Cancel an in-flight asynchronous query.
	 *
	 * @param queryId
	 *            UUID of the query to cancel
	 * @return cancellation status map
	 */
	@DeleteMapping(value = R_CUBE_QUERY, produces = MediaType.APPLICATION_JSON_VALUE)
	public Map<String, ?> cancelQuery(@RequestParam("query_id") String queryId) {
		UUID queryUuid = UUID.fromString(queryId);
		CancellationStatus status = asynchronousQueriesManager.cancelQuery(queryUuid);

		return ImmutableMap.<String, Object>builder().put("queryId", queryId).put("status", status).build();
	}

	/**
	 * Poll the result of an asynchronous query.
	 *
	 * @param queryId
	 *            UUID of the previously submitted query
	 * @param withView
	 *            if {@code true} (default), include the result view when available
	 * @return a {@link QueryResultHolder} with state and optional view
	 */
	@GetMapping(value = "/cubes/query/result", produces = MediaType.APPLICATION_JSON_VALUE)
	public QueryResultHolder fetchQueryResult(@RequestParam("query_id") String queryId,
			@RequestParam(required = false, defaultValue = "true", name = "with_view") boolean withView) {
		UUID queryUuid = UUID.fromString(queryId);
		StateAndView optView = asynchronousQueriesManager.getStateAndView(queryUuid);

		if (withView && optView.getOptView().isPresent()) {
			ListBasedTabularView view = ListBasedTabularView.load(optView.getOptView().get());
			return QueryResultHolder.served(optView.getState(), view);
		} else {
			AsynchronousStatus state = asynchronousQueriesManager.getState(queryUuid);

			Optional<Duration> optRetryIn = getRetryIn(queryUuid, state);

			if (optRetryIn.isPresent()) {
				return QueryResultHolder.retry(state, optRetryIn.get());
			} else {
				return QueryResultHolder.discarded(state);
			}
		}
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	protected Optional<Duration> getRetryIn(UUID queryId, AsynchronousStatus state) {
		return switch (state) {
		case AsynchronousStatus.DISCARDED, AsynchronousStatus.UNKNOWN, AsynchronousStatus.FAILED -> Optional.empty();
		case AsynchronousStatus.SERVED -> Optional.of(Duration.ofSeconds(0));
		case AsynchronousStatus.RUNNING -> {
			long nbPoll = queryIdPolls.getAndIncrement(queryId);

			// Exponential backoff — factor must not be too large to avoid long wait between result and poll
			double backoffFactor = 1.1;
			double exponentialBackoff = Math.pow(backoffFactor, nbPoll);
			long minRetryMs = 100L;
			long millis = (long) (minRetryMs * exponentialBackoff);
			yield Optional.of(Duration.ofMillis(millis));
		}
		};
	}

	private void serializeToArrow(IReadableTabularView view, java.io.OutputStream outputStream) throws IOException {
		ITabularViewArrowSerializer serializer = ServiceLoader.load(ITabularViewArrowSerializer.class)
				.findFirst()
				.orElseThrow(() -> new IllegalStateException(
						"No ITabularViewArrowSerializer on the classpath. Add adhoc-experimental as a runtime dependency."));

		try (WritableByteChannel channel = Channels.newChannel(outputStream)) {
			serializer.serialize(view, channel);
		}
	}

}
