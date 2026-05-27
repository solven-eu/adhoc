/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.export.excel.MeasureForestExcelLogicExporter;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

/**
 * WebFlux mirror of {@code PivotableExportController}. Returns an Excel workbook whose cells mirror the cube's measure
 * logic. See the controller's Javadoc for the contract and the v1 default-query shape.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class PivotableExportHandler {

	public static final MediaType XLSX_MEDIA_TYPE =
			MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

	protected final PivotableSchemaRegistry schemaRegistry;

	public Mono<ServerResponse> exportCubeAsExcel(ServerRequest request) {
		String endpointId = request.queryParam("endpoint_id")
				.orElseThrow(() -> new IllegalArgumentException("Missing query parameter: endpoint_id"));
		String cubeName = request.queryParam("cube")
				.orElseThrow(() -> new IllegalArgumentException("Missing query parameter: cube"));
		List<String> measures = request.queryParams().get("measure");

		UUID endpointUuid = UUID.fromString(endpointId);
		IAdhocSchema schema = schemaRegistry.getSchema(endpointUuid);
		ICubeWrapper cube = lookupCube(schema, cubeName);
		List<String> measureNames = resolveMeasureNames(cube, measures);
		CubeQuery query = CubeQuery.builder().measureNames(measureNames).build();

		byte[] bytes;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			new MeasureForestExcelLogicExporter().export(cube, query, baos);
			bytes = baos.toByteArray();
		} catch (IOException e) {
			return Mono.error(e);
		}

		return ServerResponse.ok()
				.contentType(XLSX_MEDIA_TYPE)
				.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + cubeName + ".xlsx\"")
				.body(BodyInserters.fromValue(bytes));
	}

	protected ICubeWrapper lookupCube(IAdhocSchema schema, String cubeName) {
		return schema.getCubes()
				.stream()
				.filter(c -> cubeName.equals(c.getName()))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Cube '" + cubeName + "' not found in schema"));
	}

	protected List<String> resolveMeasureNames(ICubeWrapper cube, List<String> requested) {
		if (requested != null && !requested.isEmpty()) {
			return requested;
		}
		List<String> aggregatorNames = new ArrayList<>();
		for (IMeasure measure : cube.getNameToMeasure().values()) {
			if (measure instanceof Aggregator) {
				aggregatorNames.add(measure.getName());
			}
		}
		if (aggregatorNames.isEmpty()) {
			throw new IllegalArgumentException("Cube '" + cube.getName()
					+ "' carries no Aggregator measures; pass `?measure=...` to export a specific subset.");
		}
		return aggregatorNames;
	}
}
