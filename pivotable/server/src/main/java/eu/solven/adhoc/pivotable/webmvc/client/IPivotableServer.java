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
package eu.solven.adhoc.pivotable.webmvc.client;

import java.util.List;

import eu.solven.adhoc.beta.schema.ColumnStatistics;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.dataframe.tabular.IReadableTabularView;
import eu.solven.adhoc.pivotable.endpoint.AdhocColumnSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocCoordinatesSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocEndpointSearch;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.TargetedEndpointSchemaMetadata;

/**
 * Wraps the Pivotable API in a blocking/synchronous style. Counterpart of {@link IPivotableReactiveServer}, which uses
 * Project Reactor types.
 *
 * @author Benoit Lacelle
 */
public interface IPivotableServer {
	int CACHE_TIMEOUT_MINUTES = 45;

	/** @return all endpoints matching the given search criteria. */
	List<PivotableAdhocEndpointMetadata> searchEntrypoints(AdhocEndpointSearch search);

	/** @return schemas for all endpoints matching the given search criteria. */
	List<TargetedEndpointSchemaMetadata> searchSchemas(AdhocEndpointSearch search);

	/** @return column-level statistics for columns matching the given search criteria. */
	List<ColumnStatistics> columnMetadata(AdhocColumnSearch search);

	/** @return column statistics filtered by coordinate membership criteria. */
	List<ColumnStatistics> searchMembers(AdhocCoordinatesSearch search);

	/** @return the tabular result of executing the given cube query. */
	IReadableTabularView executeQuery(TargetedCubeQuery query);
}
