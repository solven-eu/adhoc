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
package eu.solven.adhoc.cube;

import java.util.Map;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.measure.IHasMeasures;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.IHasColumns;
import eu.solven.adhoc.util.IHasName;

/**
 * A cube can execute {@link ICubeQuery}, returning an {@link ITabularView}.
 * 
 * It is generally wrapping an {@link ICubeQueryEngine} over an {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ICubeWrapper extends IHasColumns, IHasName, IHasMeasures {
	/**
	 * 
	 * @param query
	 * @return
	 */
	ITabularView execute(ICubeQuery query);

	/**
	 * 
	 * @param column
	 * @param valueMatcher
	 * @param limit
	 *            the maximum number of sample coordinates to returns
	 * @return
	 */
	default CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		CoordinatesSample coordinatesSample = getCoordinates(Map.of(column, valueMatcher), limit).get(column);

		if (coordinatesSample == null) {
			return CoordinatesSample.empty();
		}
		return coordinatesSample;
	}

	/**
	 * 
	 * @param columnToValueMatcher
	 * @param limit
	 *            the maximum number of sample coordinates to returns per column
	 * @return
	 */
	Map<String, CoordinatesSample> getCoordinates(Map<String, IValueMatcher> columnToValueMatcher, int limit);
}
