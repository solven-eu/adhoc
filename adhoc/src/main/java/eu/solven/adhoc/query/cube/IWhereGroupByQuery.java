/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.query.cube;

import java.util.List;

import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IHasFilters;
import eu.solven.adhoc.query.filter.NotFilter;

/**
 * An {@link IWhereGroupByQuery} is view of a query, not expressing its measures.
 *
 * @author Benoit Lacelle
 */
public interface IWhereGroupByQuery extends IHasFilters, IHasGroupBy {

	/**
	 * The filter of current query. A filter refers to the condition for the data to be included. An AND over an empty
	 * {@link List} means the whole data has to be included. Exclusions can be done through {@link NotFilter}
	 *
	 * @return a list of filters (to be interpreted as an OR over AND simple conditions).
	 */
	@Override
	IAdhocFilter getFilter();

	/**
	 * The columns amongst which the result has to be ventilated/sliced.
	 *
	 * @return a Set of columns
	 */
	@Override
	IAdhocGroupBy getGroupBy();
}