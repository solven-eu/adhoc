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
package eu.solven.adhoc.coordinate;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.query.filter.IAdhocFilter;

/**
 * A Coordinate is a possible value along an {@link IAdhocColumn}. While most coordinate are defined as distinct values,
 * as would be reported by a `GROUP BY`, some coordinates can be defined programmatically relatively to current column.
 * 
 * @author Benoit Lacelle
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS,
		include = JsonTypeInfo.As.PROPERTY,
		property = "type",
		defaultImpl = CalculatedCoordinate.class)
public interface ICalculatedCoordinate {
	/**
	 * A calculated coordinate representing all available coordinates. If current column is filtered, this would include
	 * only the filtered members.
	 */
	String COORDINATE_STAR = "*";

	Object getCoordinate();

	IAdhocFilter getFilter();
}
