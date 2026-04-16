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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import eu.solven.adhoc.beta.schema.ColumnIdentifier;
import eu.solven.adhoc.beta.schema.ColumnStatistics;
import eu.solven.adhoc.beta.schema.ColumnarMetadata;
import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.beta.schema.CubeSchemaMetadata;
import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;
import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.endpoint.AdhocColumnSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocEndpointSearch;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.PivotableEndpointsRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import eu.solven.adhoc.pivotable.endpoint.TargetedEndpointSchemaMetadata;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * API for discovering available endpoints, their schemas, and column metadata.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
@RestController
@RequestMapping(IPivotableApiConstants.PREFIX)
public class PivotableEndpointsController {

	private static final int DEFAULT_LIMIT_COORDINATES = 100;

	final PivotableEndpointsRegistry endpointsRegistry;
	final PivotableSchemaRegistry schemasRegistry;

	/**
	 * @param endpointId
	 *            optional filter by endpoint UUID
	 * @param keyword
	 *            optional keyword search
	 * @return list of matching endpoints
	 */
	@GetMapping(value = "/endpoints", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<PivotableAdhocEndpointMetadata> listEntrypoints(
			@RequestParam(required = false, name = "endpoint_id") String endpointId,
			@RequestParam(required = false) String keyword) {
		List<PivotableAdhocEndpointMetadata> endpoints = matchingEndpoints(endpointId, keyword);
		log.debug("Entrypoints for endpointId={} keyword={}: {}", endpointId, keyword, endpoints);
		return endpoints;
	}

	/**
	 * @param endpointId
	 *            optional filter by endpoint UUID
	 * @param keyword
	 *            optional keyword search
	 * @param table
	 *            optional table filter
	 * @param cube
	 *            optional cube filter
	 * @return list of endpoint schemas
	 */
	@GetMapping(value = "/endpoints/schemas", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<TargetedEndpointSchemaMetadata> endpointSchema(
			@RequestParam(required = false, name = "endpoint_id") String endpointId,
			@RequestParam(required = false) String keyword,
			@RequestParam(required = false) String table,
			@RequestParam(required = false) String cube) {
		return matchingSchema(endpointId, keyword, table, cube, true);
	}

	/**
	 * @param endpointId
	 *            optional filter by endpoint UUID
	 * @param keyword
	 *            optional keyword search
	 * @param table
	 *            optional table to restrict column search
	 * @param cube
	 *            optional cube to restrict column search
	 * @param name
	 *            optional column name filter (exact match)
	 * @param coordinate
	 *            optional coordinate filter (not yet supported)
	 * @param limitCoordinates
	 *            max coordinates per column; defaults to {@value #DEFAULT_LIMIT_COORDINATES}
	 * @return list of matching column statistics
	 */
	@GetMapping(value = "/endpoints/schemas/columns", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<ColumnStatistics> searchColumns(@RequestParam(required = false, name = "endpoint_id") String endpointId,
			@RequestParam(required = false) String keyword,
			@RequestParam(required = false) String table,
			@RequestParam(required = false) String cube,
			@RequestParam(required = false) String name,
			@RequestParam(required = false) String coordinate,
			@RequestParam(required = false) Integer limitCoordinates) {
		AdhocColumnSearch.AdhocColumnSearchBuilder parameters = AdhocColumnSearch.builder();

		if (table != null) {
			parameters.table(Optional.of(table));
		}
		if (cube != null) {
			parameters.cube(Optional.of(cube));
		}
		if (name != null) {
			parameters.name(Optional.of(EqualsMatcher.matchEq(name)));
		}
		if (coordinate != null) {
			parameters.coordinate(Optional.of(EqualsMatcher.matchEq(coordinate)));
		}
		parameters.limitCoordinates(Optional.ofNullable(limitCoordinates).orElse(DEFAULT_LIMIT_COORDINATES));

		AdhocColumnSearch columnSearch = parameters.build();

		columnSearch.getCoordinate().ifPresent(c -> {
			throw new NotYetImplementedException("Searching for columns given coordinate=%s".formatted(c));
		});

		if (columnSearch.getTable().isEmpty() && columnSearch.getCube().isEmpty()) {
			throw new NotYetImplementedException("Need to explicit a table or a cube");
		}

		List<TargetedEndpointSchemaMetadata> schemas = matchingSchema(endpointId, keyword, table, cube, false);

		List<ColumnStatistics> matchingColumns = schemas.stream().flatMap(endpointSchema -> {
			EndpointSchemaMetadata schema = endpointSchema.getSchema();

			List<ColumnStatistics> endpointColumns = new ArrayList<>();

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
		return matchingColumns;
	}

	private List<PivotableAdhocEndpointMetadata> matchingEndpoints(String endpointId, String keyword) {
		AdhocEndpointSearch.AdhocEndpointSearchBuilder parameters = AdhocEndpointSearch.builder();

		if (endpointId != null) {
			parameters.endpointId(Optional.of(UUID.fromString(endpointId)));
		}
		if (keyword != null) {
			parameters.keyword(Optional.of(keyword));
		}

		return endpointsRegistry.search(parameters.build());
	}

	private List<TargetedEndpointSchemaMetadata> matchingSchema(String endpointId,
			String keyword,
			String table,
			String cube,
			boolean allIfEmpty) {
		List<PivotableAdhocEndpointMetadata> endpoints = matchingEndpoints(endpointId, keyword);

		IAdhocSchema.AdhocSchemaQuery.AdhocSchemaQueryBuilder queryBuilder = IAdhocSchema.AdhocSchemaQuery.builder();
		if (table != null) {
			queryBuilder.table(Optional.of(table));
		}
		if (cube != null) {
			queryBuilder.cube(Optional.of(cube));
		}
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

	protected List<ColumnStatistics> addMatchingColumns(TargetedEndpointSchemaMetadata schemaMetadata,
			ColumnIdentifier columnTemplate,
			AdhocColumnSearch columnSearch,
			ColumnarMetadata holderColumns) {
		List<ColumnStatistics> columns = new ArrayList<>();

		if (holderColumns != null) {
			UUID endpointId = schemaMetadata.getEndpoint().getId();

			IAdhocSchema schema = schemasRegistry.getSchema(endpointId);

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
