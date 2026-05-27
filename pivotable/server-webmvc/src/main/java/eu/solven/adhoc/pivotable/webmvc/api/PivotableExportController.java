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
package eu.solven.adhoc.pivotable.webmvc.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.export.excel.MeasureForestExcelLogicExporter;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import eu.solven.adhoc.pivotable.webnone.api.IPivotableRouteConstants;
import lombok.RequiredArgsConstructor;

/**
 * Returns an Excel workbook whose cells mirror the cube's measure logic. Leaf {@code Aggregator} columns carry the
 * engine-computed numeric value; {@code Combinator} columns carry an Excel formula referencing other cells in the same
 * row. Opening the workbook reproduces the engine's output and lets a consumer modify leaf values to see the downstream
 * impact recompute Excel-natively.
 *
 * <p>
 * Wired only when {@link MeasureForestExcelLogicExporter} (shipped by the optional {@code adhoc-experimental} jar) is
 * on the classpath at runtime. Projects that omit {@code adhoc-experimental} simply don't expose the endpoint.
 *
 * <p>
 * v1 default-query shape: when {@code measure} is unspecified, the controller queries every {@link Aggregator} in the
 * cube's forest at grand-total. That gives a useful "every leaf value" starting export. Callers wanting specific
 * measures (e.g. a Combinator chain) pass them via repeated {@code ?measure=…} parameters; the exporter walks each down
 * through its underlyings and emits one column per reachable measure.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@RestController
@RequestMapping(IPivotableApiConstants.PREFIX)
@ConditionalOnClass(MeasureForestExcelLogicExporter.class)
public class PivotableExportController implements IPivotableRouteConstants {

	public static final MediaType XLSX_MEDIA_TYPE =
			MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

	protected final PivotableSchemaRegistry schemaRegistry;

	@GetMapping(R_CUBE_EXPORT_EXCEL)
	public ResponseEntity<byte[]> exportCubeAsExcel(@RequestParam("endpoint_id") String endpointId,
			@RequestParam("cube") String cubeName,
			@RequestParam(name = "measure", required = false) List<String> measures) throws IOException {
		UUID endpointUuid = UUID.fromString(endpointId);
		IAdhocSchema schema = schemaRegistry.getSchema(endpointUuid);
		ICubeWrapper cube = lookupCube(schema, cubeName);
		List<String> measureNames = resolveMeasureNames(cube, measures);
		CubeQuery query = CubeQuery.builder().measureNames(measureNames).build();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new MeasureForestExcelLogicExporter().export(cube, query, baos);

		return ResponseEntity.ok()
				.contentType(XLSX_MEDIA_TYPE)
				.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + cubeName + ".xlsx\"")
				.body(baos.toByteArray());
	}

	protected ICubeWrapper lookupCube(IAdhocSchema schema, String cubeName) {
		return schema.getCubes()
				.stream()
				.filter(c -> cubeName.equals(c.getName()))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Cube '" + cubeName + "' not found in schema"));
	}

	/**
	 * If the caller specified measures, honour them verbatim. Otherwise default to every {@link Aggregator} in the
	 * forest — the simplest non-empty starting export that respects the v1 "Aggregator + Combinator only" scope.
	 */
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
