/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.case_insensitivive;

import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.MoreFilterHelpers;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManagerSimple;
import eu.solven.adhoc.table.transcoder.value.StandardCustomTypeManager;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A decorating {@link ICubeQueryEngine} that applies case-insensitive column name normalization before delegating to
 * the wrapped engine.
 *
 * <p>
 * On each {@link #execute(QueryPod)}, the table schema columns are registered into a fresh
 * {@link CaseInsensitiveContext}. Filter column names and groupBy column names in the query are then rewritten to their
 * canonical (first-registered) casing before the delegate engine processes them.
 *
 * @author Benoit Lacelle
 */
@Builder
@RequiredArgsConstructor
@Deprecated(
		since = "Not-Ready. Does not handle ITableWrapper with mixed case. Does not handle measure with groupBy/filter with mixed case")
public class CaseInsensitiveCubeQueryEngine implements ICubeQueryEngine {

	// Value transcoding is not part of case-insensitivity: column names are renamed, values are left untouched
	private static final ICustomTypeManagerSimple NOOP_TYPE_MANAGER = new StandardCustomTypeManager();

	@NonNull
	final ICubeQueryEngine delegate;

	@Override
	public ITabularView execute(QueryPod queryPod) {
		QueryPod normalized = normalizeForCaseInsensitivity(queryPod);
		return delegate.execute(normalized);
	}

	/**
	 * Rewrites filter and groupBy column names to the canonical casing declared by the table schema.
	 */
	protected QueryPod normalizeForCaseInsensitivity(QueryPod queryPod) {
		CaseInsensitiveContext ctx = new CaseInsensitiveContext();
		queryPod.getTable().getColumns().forEach(col -> ctx.registerColumn(col.getName()));

		ITableAliaser aliaser = ctx::normalize;

		ICubeQuery query = queryPod.getQuery();
		ISliceFilter normalizedFilter =
				MoreFilterHelpers.transcodeFilter(NOOP_TYPE_MANAGER, aliaser, query.getFilter());
		IGroupBy normalizedGroupBy =
				GroupByColumns.named(query.getGroupBy().getGroupedByColumns().stream().map(ctx::normalize).toList());

		ICubeQuery normalizedQuery = CubeQuery.edit(query).filter(normalizedFilter).groupBy(normalizedGroupBy).build();
		return queryPod.toBuilder().query(normalizedQuery).build();
	}
}
