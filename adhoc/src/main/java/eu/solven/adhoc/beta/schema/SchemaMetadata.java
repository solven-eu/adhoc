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
package eu.solven.adhoc.beta.schema;

import java.util.List;
import java.util.Map;

import eu.solven.adhoc.cube.IAdhocCubeWrapper;
import eu.solven.adhoc.measure.IAdhocMeasureBag;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * A schema describing metadata for a set of {@link IAdhocTableWrapper}, {@link IAdhocMeasureBag},
 * {@link IAdhocCubeWrapper} and {@link IAdhocQuery}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class SchemaMetadata {

	@Singular
	Map<String, ColumnarMetadata> tableToColumns;

	@Singular
	Map<String, List<IMeasure>> bagToMeasures;

	@Singular
	Map<String, ColumnarMetadata> cubeToColumns;

	@Singular
	Map<String, AdhocQuery> nameToQueries;

	@Singular
	Map<String, Object> nameToCustomMarkers;
}
