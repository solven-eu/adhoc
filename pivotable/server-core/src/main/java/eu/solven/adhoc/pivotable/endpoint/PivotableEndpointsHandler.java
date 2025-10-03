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
package eu.solven.adhoc.pivotable.endpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.beta.schema.ColumnIdentifier;
import eu.solven.adhoc.beta.schema.ColumnStatistics;
import eu.solven.adhoc.beta.schema.ColumnarMetadata;
import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.beta.schema.CubeSchemaMetadata;
import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;
import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.pivotable.webflux.api.AdhocHandlerHelper;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Handler related to {@link PivotableAdhocEndpointMetadata} and related
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PivotableEndpointsHandler {
	private static final int DEFAULT_LIMIT_COORDINATES = 100;

	final PivotableEndpointsRegistry endpointsRegistry;
	final PivotableAdhocSchemaRegistry schemasRegistry;

	private List<PivotableAdhocEndpointMetadata> matchingEndpoints(ServerRequest request) {
		AdhocEndpointSearch.AdhocEndpointSearchBuilder parameters = AdhocEndpointSearch.builder();

		AdhocHandlerHelper.optUuid(request, "endpoint_id").ifPresent(id -> parameters.endpointId(Optional.of(id)));

		Optional<String> optKeyword = request.queryParam("keyword");
		optKeyword.ifPresent(rawKeyword -> parameters.keyword(Optional.of(rawKeyword)));

		List<PivotableAdhocEndpointMetadata> endpoints = endpointsRegistry.search(parameters.build());
		log.debug("Entrypoints for {}: {}", parameters, endpoints);
		return endpoints;
	}

	public Mono<ServerResponse> listEntrypoints(ServerRequest request) {
		List<PivotableAdhocEndpointMetadata> endpoints = matchingEndpoints(request);
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(endpoints));
	}

	public Mono<ServerResponse> endpointSchema(ServerRequest request) {
		// If no cube or table is specific, returns everything
		List<TargetedEndpointSchemaMetadata> schemas = matchingSchema(request, true);
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(schemas));
	}

	/**
	 *
	 * @param request
	 * @return a {@link List} of {@link TargetedEndpointSchemaMetadata}, filtered the optionally filtered table and
	 *         cube.
	 */
	protected List<TargetedEndpointSchemaMetadata> matchingSchema(ServerRequest request, boolean allIfEmpty) {
		List<PivotableAdhocEndpointMetadata> endpoints = matchingEndpoints(request);

		IAdhocSchema.AdhocSchemaQuery.AdhocSchemaQueryBuilder queryBuilder = IAdhocSchema.AdhocSchemaQuery.builder();
		AdhocHandlerHelper.optString(request, "table").ifPresent(id -> queryBuilder.table(Optional.of(id)));
		AdhocHandlerHelper.optString(request, "cube").ifPresent(id -> queryBuilder.cube(Optional.of(id)));
		IAdhocSchema.AdhocSchemaQuery query = queryBuilder.build();

		List<TargetedEndpointSchemaMetadata> schemas = endpoints.stream().map(endpoint -> {
			if (!"http://localhost:self".equals(endpoint.getUrl())) {
				throw new NotYetImplementedException("%s".formatted(PepperLogHelper.getObjectAndClass(endpoint)));
			}

			EndpointSchemaMetadata schemaMetadata;
			try {
				schemaMetadata = schemasRegistry.getSchema(endpoint.getId()).getMetadata(query, allIfEmpty);
			} catch (Exception e) {
				if (AdhocUnsafe.isFailFast()) {
					throw new IllegalStateException("Issue loading schema for endpoint=%s".formatted(endpoint), e);
				} else {
					log.warn("Issue loading schema for endpoint={}", endpoint, e);
					// Loading Schema may fail as we load all Combinations for IColumnGenerators, and any configuration
					// issue leads to a failure
					schemaMetadata = EndpointSchemaMetadata.builder()
							.cube("error-" + e.getMessage(), CubeSchemaMetadata.builder().build())
							.build();
				}
			}
			return TargetedEndpointSchemaMetadata.builder().endpoint(endpoint).schema(schemaMetadata).build();
		}).toList();

		log.debug("Schemas for {}: {}", endpoints, schemas);
		return schemas;
	}

	public Mono<ServerResponse> searchColumns(ServerRequest request) {

		AdhocColumnSearch.AdhocColumnSearchBuilder parameters = AdhocColumnSearch.builder();

		AdhocHandlerHelper.optString(request, "table").ifPresent(id -> parameters.table(Optional.of(id)));
		AdhocHandlerHelper.optString(request, "cube").ifPresent(id -> parameters.cube(Optional.of(id)));

		AdhocHandlerHelper.optString(request, "name")
				.ifPresent(id -> parameters.name(Optional.of(EqualsMatcher.matchEq(id))));
		// TODO How to turn from String to Object? (e.g. LocalDate)
		// Should we switch to a `SameString` matcher?
		AdhocHandlerHelper.optString(request, "coordinate")
				.ifPresent(id -> parameters.coordinate(Optional.of(EqualsMatcher.matchEq(id))));

		Number limitCoordinates =
				AdhocHandlerHelper.optNumber(request, "limit_coordinates").orElse(DEFAULT_LIMIT_COORDINATES);
		parameters.limitCoordinates(limitCoordinates.intValue());

		AdhocColumnSearch columnSearch = parameters.build();

		columnSearch.getCoordinate().ifPresent(coordinate -> {
			throw new NotYetImplementedException("Searching for columns given coordinate=%s".formatted(coordinate));
		});

		if (columnSearch.getTable().isEmpty() && columnSearch.getCube().isEmpty()) {
			throw new NotYetImplementedException("Need to explicit a table or acube");
		}

		// Request columns only for expressed cube and table
		List<TargetedEndpointSchemaMetadata> schemas = matchingSchema(request, false);

		List<ColumnStatistics> matchingColumns = schemas.stream().flatMap(endpointSchema -> {
			EndpointSchemaMetadata schema = endpointSchema.getSchema();

			List<ColumnStatistics> endpointColumns = new ArrayList<>();

			// If neither table nor cube, search all?
			columnSearch.getTable().ifPresent(tableName -> {
				ColumnarMetadata tableColumns = schema.getTables().get(tableName);

				ColumnIdentifier columnId =
						ColumnIdentifier.builder().isCubeElseTable(false).holder(tableName).column("searched").build();

				endpointColumns.addAll(addMatchingColumns(endpointSchema, columnId, columnSearch, tableColumns));
			});

			columnSearch.getCube().ifPresent(cubeName -> {
				ColumnarMetadata cubeColumns = schema.getCubes().get(cubeName).getColumns();

				ColumnIdentifier columnId =
						ColumnIdentifier.builder().isCubeElseTable(true).holder(cubeName).column("searched").build();

				endpointColumns.addAll(addMatchingColumns(endpointSchema, columnId, columnSearch, cubeColumns));
			});

			return endpointColumns.stream();
		}).toList();

		log.debug("Columns for {}: {}", columnSearch, matchingColumns);
		return ServerResponse.ok()
				.contentType(MediaType.APPLICATION_JSON)
				.body(BodyInserters.fromValue(matchingColumns));
	}

	protected List<ColumnStatistics> addMatchingColumns(TargetedEndpointSchemaMetadata schemaMetadata,
			ColumnIdentifier columnTemplate,
			AdhocColumnSearch columnSearch,
			ColumnarMetadata holderColumns) {
		List<ColumnStatistics> columns = new ArrayList<>();

		if (holderColumns != null) {
			UUID endpointId = schemaMetadata.getEndpoint().getId();

			AdhocSchema schema = schemasRegistry.getSchema(endpointId);

			Map<String, ? extends Map<String, ?>> columnToDetails = holderColumns.getColumns();

			columnToDetails.entrySet()
					.stream()
					.filter(e -> columnSearch.getName().isEmpty() || columnSearch.getName().get().match(e.getKey()))
					.sorted(Map.Entry.comparingByKey())
					.forEach(e -> {
						String column = e.getKey();
						Map<String, ?> columnDetails = e.getValue();

						ColumnIdentifier columnId = columnTemplate.toBuilder().column(column).build();

						CoordinatesSample coordinates = schema.getCoordinates(columnId,
								columnSearch.getCoordinate().orElse(IValueMatcher.MATCH_ALL),
								columnSearch.getLimitCoordinates());

						columns.add(ColumnStatistics.builder()
								.entrypointId(endpointId)
								.holder(columnTemplate.getHolder())
								.column(column)
								.type(MapPathGet.getRequiredString(columnDetails, "type"))
								.tags(MapPathGet.getRequiredAs(columnDetails, "tags"))
								.coordinates(coordinates.getCoordinates())
								.estimatedCardinality(coordinates.getEstimatedCardinality())
								.build());
					});
		}

		return columns;
	}

}