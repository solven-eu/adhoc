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
package eu.solven.adhoc.engine.step;

import java.util.Set;

import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * Enables reading coordinates/column value given a {@link ISliceWithStep}.
 * 
 * While a {@link ISliceWithStep} can be interpreted as a {@link ISliceFilter}, doing so may be inefficient. This object
 * helps getting coordinates in an optimal way.
 * 
 * @author Benoit Lacelle
 */
public interface ISliceReader {
	/**
	 * This is a very generic behavior, but it may be quite slow.
	 * 
	 * @return this as a {@link ISliceFilter}.
	 */
	ISliceFilter asFilter();

	/**
	 * 
	 * Follows the semantic of {@link FilterHelpers#getValueMatcher(ISliceFilter, String)}.
	 * 
	 * @param column
	 * @return a perfectly matching {@link IValueMatcher} for given column.
	 */
	IValueMatcher getValueMatcher(String column);

	/**
	 * Follows the semantic of {@link FilterHelpers#getValueMatcherLax(ISliceFilter, String)}.
	 * 
	 * @param column
	 * @return a lax matching {@link IValueMatcher} for given column.
	 */
	IValueMatcher getValueMatcherLax(String column);

	/**
	 * 
	 * @return the Set of column with an expressed filter.
	 */
	Set<String> getFilteredColumns();

}
