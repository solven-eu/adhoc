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

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.export.excel.MeasureForestExcelLogicExporter;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import eu.solven.adhoc.pivotable.webnone.api.IPivotableRouteConstants;

/**
 * Loaded only when {@link MeasureForestExcelLogicExporter} (from the optional {@code adhoc-experimental} jar) is on the
 * classpath. Registers the {@code GET /api/v1/cubes/export/excel} route under its own {@link RouterFunction}, which
 * Spring composes with the main {@code PivotableApiRouter}.
 *
 * @author Benoit Lacelle
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(MeasureForestExcelLogicExporter.class)
public class PivotableExportConfig {

	@Bean
	public PivotableExportHandler pivotableExportHandler(PivotableSchemaRegistry schemaRegistry) {
		return new PivotableExportHandler(schemaRegistry);
	}

	@Bean
	public RouterFunction<ServerResponse> excelExportRoutes(PivotableExportHandler handler) {
		return RouterFunctions.route()
				.GET(IPivotableApiConstants.PREFIX + IPivotableRouteConstants.R_CUBE_EXPORT_EXCEL,
						handler::exportCubeAsExcel)
				.build();
	}
}
