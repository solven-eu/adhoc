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
package eu.solven.adhoc.engine.context;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

@Builder
public class DefaultQueryPreparator implements IQueryPreparator {

	// By default, the filters are not modified
	@NonNull
	@Default
	final IImplicitFilter implicitFilter = query -> IAdhocFilter.MATCH_ALL;

	@NonNull
	@Default
	final IImplicitOptions implicitOptions = query -> ImmutableSet.of();

	@NonNull
	@Default
	final ExecutorService concurrentExecutorService = AdhocUnsafe.adhocCommonPool;

	// If not-concurrent, we want the simpler execution plan as possible
	// it is especially important to simplify thread-jumping lowering readability of stack-traces
	@NonNull
	@Default
	final ExecutorService nonConcurrentExecutorService = MoreExecutors.newDirectExecutorService();

	@Override
	public QueryPod prepareQuery(ITableWrapper table,
			IMeasureForest forest,
			IColumnsManager columnsManager,
			ICubeQuery rawQuery) {
		ICubeQuery preparedQuery = combineWithImplicit(rawQuery);
		AdhocQueryId queryId = AdhocQueryId.from(table.getName(), preparedQuery);

		return QueryPod.builder()
				.query(preparedQuery)
				.queryId(queryId)
				.forest(forest)
				.table(table)
				.columnsManager(columnsManager)
				.executorService(getExecutorService(preparedQuery))
				.build();
	}

	protected ExecutorService getExecutorService(ICubeQuery preparedQuery) {
		if (preparedQuery.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
			// Concurrent query: execute in a dedicated pool
			return concurrentExecutorService;
		} else {
			// Not concurrent query: rely on current thread
			return nonConcurrentExecutorService;
		}
	}

	protected ICubeQuery combineWithImplicit(ICubeQuery rawQuery) {
		IAdhocFilter preprocessedFilter =
				AndFilter.and(rawQuery.getFilter(), implicitFilter.getImplicitFilter(rawQuery));

		Set<IQueryOption> addedOptions = implicitOptions.getOptions(rawQuery);
		CubeQuery query = CubeQuery.edit(rawQuery).filter(preprocessedFilter).options(addedOptions).build();

		if (rawQuery instanceof AdhocSubQuery subQuery) {
			return subQuery.toBuilder().subQuery(query).build();
		} else {
			return query;
		}
	}

}
