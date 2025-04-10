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
package eu.solven.adhoc.dag;

import eu.solven.adhoc.column.IAdhocColumnsManager;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * Generate {@link ExecutingQueryContext} given an {@link IAdhocQuery} and its context of execution.
 * 
 * @author Benoit Lacelle
 * 
 * @see IAdhocColumnsManager
 * @see IAdhocImplicitFilter
 */
public interface IQueryPreparator {
	/**
	 * 
	 * @param table
	 * @param forest
	 * @param columnsManager
	 * @param query
	 * @return an {@link ExecutingQueryContext} which wraps together everything necessary to execute a query.
	 */
	ExecutingQueryContext prepareQuery(ITableWrapper table,
			IMeasureForest forest,
			IAdhocColumnsManager columnsManager,
			IAdhocQuery query);
}
