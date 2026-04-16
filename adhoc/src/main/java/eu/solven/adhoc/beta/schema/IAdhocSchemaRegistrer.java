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

import java.util.Set;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * Enables modifying an {@link IAdhocSchema}.
 * 
 * @author Benoit Lacelle
 */
public interface IAdhocSchemaRegistrer {

	void registerForest(IMeasureForest fromMeasures);

	void registerCustomMarker(String cube,
			IValueMatcher matchEq,
			CustomMarkerMetadataGenerator customMarkerMetadataGenerator);

	void registerTable(ITableWrapper table);

	void registerCube(ICubeWrapper cube);

	CubeWrapper registerCube(String cubeName, String tableName, String forestName);

	CubeWrapper.CubeWrapperBuilder openCubeWrapperBuilder();

	void tagColumn(ColumnIdentifier columnIdentifier, Set<String> tags);

	void tagMeasure(MeasureIdentifier measureIdentifier, Set<String> tags);

}
