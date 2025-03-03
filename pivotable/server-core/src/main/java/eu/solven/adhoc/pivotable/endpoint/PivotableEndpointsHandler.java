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
import java.util.Set;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.beta.schema.ColumnIdentifier;
import eu.solven.adhoc.beta.schema.ColumnMetadata;
import eu.solven.adhoc.beta.schema.ColumnarMetadata;
import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;
import eu.solven.adhoc.pivotable.webflux.api.AdhocHandlerHelper;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
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
		List<TargetedEndpointSchemaMetadata> schemas = matchingSchema(request);
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(schemas));
	}

	private List<TargetedEndpointSchemaMetadata> matchingSchema(ServerRequest request) {
		List<PivotableAdhocEndpointMetadata> endpoints = matchingEndpoints(request);

		List<TargetedEndpointSchemaMetadata> schemas = endpoints.stream().map(endpoint -> {
			if (!"http://localhost:self".equals(endpoint.getUrl())) {
				throw new NotYetImplementedException("%s".formatted(PepperLogHelper.getObjectAndClass(endpoint)));
			}

			return TargetedEndpointSchemaMetadata.builder()
					.endpoint(endpoint)
					.schema(schemasRegistry.getSchema(endpoint.getId()).getMetadata())
					.build();
		}).toList();

		log.debug("Schemas for {}: {}", endpoints, schemas);
		return schemas;
	}

	public Mono<ServerResponse> searchColumns(ServerRequest request) {
		List<TargetedEndpointSchemaMetadata> schemas = matchingSchema(request);

		AdhocColumnSearch.AdhocColumnSearchBuilder parameters = AdhocColumnSearch.builder();

		AdhocHandlerHelper.optString(request, "table").ifPresent(id -> parameters.table(Optional.of(id)));
		AdhocHandlerHelper.optString(request, "cube").ifPresent(id -> parameters.cube(Optional.of(id)));

		AdhocHandlerHelper.optString(request, "name")
				.ifPresent(id -> parameters.name(Optional.of(EqualsMatcher.isEqualTo(id))));
		// TODO How to turn from String to Object? (e.g. LocalDate)
		AdhocHandlerHelper.optString(request, "coordinate")
				.ifPresent(id -> parameters.coordinate(Optional.of(EqualsMatcher.isEqualTo(id))));

		AdhocColumnSearch columnSearch = parameters.build();

		columnSearch.getCoordinate().ifPresent(coordinate -> {
			throw new NotYetImplementedException("Searching for columns given coordinate=%s".formatted(coordinate));
		});

		if (columnSearch.getTable().isEmpty() && columnSearch.getCube().isEmpty()) {
			throw new NotYetImplementedException("Need to explicit a table or acube");
		}

		List<ColumnMetadata> matchingColumns = schemas.stream().flatMap(endpointSchema -> {
			EndpointSchemaMetadata schema = endpointSchema.getSchema();

			List<ColumnMetadata> endpointColumns = new ArrayList<>();

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
						ColumnIdentifier.builder().isCubeElseTable(false).holder(cubeName).column("searched").build();

				endpointColumns.addAll(addMatchingColumns(endpointSchema, columnId, columnSearch, cubeColumns));
			});

			return endpointColumns.stream();
		}).toList();

		log.debug("Columns for {}: {}", columnSearch, matchingColumns);
		return ServerResponse.ok()
				.contentType(MediaType.APPLICATION_JSON)
				.body(BodyInserters.fromValue(matchingColumns));
	}

	protected List<ColumnMetadata> addMatchingColumns(TargetedEndpointSchemaMetadata schemaMetadata,
			ColumnIdentifier tableId,
			AdhocColumnSearch columnSearch,
			ColumnarMetadata holderColumns) {
		List<ColumnMetadata> columns = new ArrayList<>();

		if (holderColumns != null) {
			UUID endpointId = schemaMetadata.getEndpoint().getId();

			AdhocSchema schema = schemasRegistry.getSchema(endpointId);

			Map<String, String> columnToTypes = holderColumns.getColumnToTypes();
			// columnSearch.getName().ifPresent(searchedColumnName -> {

			columnToTypes.entrySet()
					.stream()
					.filter(e -> columnSearch.getName().isEmpty() || columnSearch.getName().get().match(e.getKey()))
					.forEach(e -> {
						String column = e.getKey();
						String type = e.getValue();

						ColumnIdentifier columnId = tableId.toBuilder().column(column).build();

						Set<?> coordinates = schema.getCoordinates(columnId);

						columns.add(ColumnMetadata.builder()
								.entrypointId(endpointId)
								.holder(tableId.getHolder())
								.column(column)
								.type(type)
								.coordinates(coordinates)
								.estimatedCardinality(coordinates.size())
								.build());
					});
			// });
		}

		return columns;
	}

}