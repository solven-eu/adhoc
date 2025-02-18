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
package eu.solven.adhoc.dag;

import java.util.Set;

import eu.solven.adhoc.column.AdhocColumnsManager;
import eu.solven.adhoc.column.IAdhocColumnsManager;
import eu.solven.adhoc.measure.IAdhocMeasureBag;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.table.IAdhocTableWrapper;

/**
 * Holds the logic to execute a query, which means turning a {@link IAdhocQuery} into a {@link ITabularView}.
 *
 * @author Benoit Lacelle
 */
public interface IAdhocQueryEngine {

	/**
	 * Execute an {@link IAdhocQuery}.
	 *
	 * @param executingQueryContext
	 * @return
	 */
	ITabularView execute(ExecutingQueryContext executingQueryContext);

	default ITabularView execute(IAdhocQuery query, IAdhocMeasureBag measures, IAdhocTableWrapper table) {
		return execute(query, Set.of(), measures, table, AdhocColumnsManager.builder().build());
	}

	default ITabularView execute(IAdhocQuery query,
			Set<? extends IQueryOption> options,
			IAdhocMeasureBag measures,
			IAdhocTableWrapper table,
			IAdhocColumnsManager columnsManager) {
		ExecutingQueryContext executingQueryContext = ExecutingQueryContext.builder()
				.query(query)
				.options(options)
				.measures(measures)
				.table(table)
				.columnsManager(columnsManager)
				.build();
		return execute(executingQueryContext);
	}
}
