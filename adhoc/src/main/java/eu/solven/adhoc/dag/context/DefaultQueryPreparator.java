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
package eu.solven.adhoc.dag.context;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.table.ITableWrapper;
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

	@Override
	public ExecutingQueryContext prepareQuery(ITableWrapper table,
			IMeasureForest forest,
			IColumnsManager columnsManager,
			IAdhocQuery rawQuery) {
		IAdhocQuery preparedQuery = combineWithImplicit(rawQuery);
		AdhocQueryId queryId = AdhocQueryId.from(table.getName(), preparedQuery);

		return ExecutingQueryContext.builder()
				.query(preparedQuery)
				.queryId(queryId)
				.forest(forest)
				.table(table)
				.columnsManager(columnsManager)
				.build();
	}

	protected IAdhocQuery combineWithImplicit(IAdhocQuery rawQuery) {
		IAdhocFilter preprocessedFilter =
				AndFilter.and(rawQuery.getFilter(), implicitFilter.getImplicitFilter(rawQuery));

		Set<IQueryOption> addedOptions = implicitOptions.getOptions(rawQuery);
		AdhocQuery query = AdhocQuery.edit(rawQuery).filter(preprocessedFilter).options(addedOptions).build();

		if (rawQuery instanceof AdhocSubQuery subQuery) {
			return subQuery.toBuilder().subQuery(query).build();
		} else {
			return query;
		}
	}

}
