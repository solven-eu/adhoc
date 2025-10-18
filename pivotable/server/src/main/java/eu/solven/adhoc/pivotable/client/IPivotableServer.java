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
package eu.solven.adhoc.pivotable.client;

import eu.solven.adhoc.beta.schema.ColumnStatistics;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.data.tabular.IReadableTabularView;
import eu.solven.adhoc.pivotable.endpoint.AdhocColumnSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocCoordinatesSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocEndpointSearch;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.TargetedEndpointSchemaMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Wraps Pivotable API
 * 
 * @author Benoit Lacelle
 *
 */
public interface IPivotableServer {
	Flux<PivotableAdhocEndpointMetadata> searchEntrypoints(AdhocEndpointSearch search);

	Flux<TargetedEndpointSchemaMetadata> searchSchemas(AdhocEndpointSearch search);

	Flux<ColumnStatistics> columnMetadata(AdhocColumnSearch search);

	Flux<ColumnStatistics> searchMembers(AdhocCoordinatesSearch search);

	Mono<IReadableTabularView> executeQuery(TargetedCubeQuery query);

}
