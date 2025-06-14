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
package eu.solven.adhoc.engine;

import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.engine.context.IQueryPreparator;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * Holds the logic to execute a query, which means turning a {@link ICubeQuery} into a {@link ITabularView}.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface ICubeQueryEngine {

	/**
	 * Execute an {@link ICubeQuery}.
	 *
	 * @param queryPod
	 * @return
	 */
	ITabularView execute(QueryPod queryPod);

	@Deprecated(since = "This use a default IAdhocImplicitFilter")
	default ITabularView executeUnsafe(ICubeQuery query, IMeasureForest measures, ITableWrapper table) {
		return executeUnsafe(query, measures, table, ColumnsManager.builder().build());
	}

	@Deprecated(since = "This use a default IAdhocImplicitFilter")
	default ITabularView executeUnsafe(ICubeQuery query,
			IMeasureForest measures,
			ITableWrapper table,
			IColumnsManager columnsManager) {
		IQueryPreparator queryPreparator = StandardQueryPreparator.builder().build();

		QueryPod queryPod = queryPreparator.prepareQuery(table, measures, columnsManager, query);
		return execute(queryPod);
	}
}
