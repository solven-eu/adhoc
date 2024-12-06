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
package eu.solven.adhoc.query;

import java.util.Collections;
import java.util.Set;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IHasFilters;
import eu.solven.adhoc.api.v1.IHasGroupBy;
import eu.solven.adhoc.api.v1.IIsDebugable;
import eu.solven.adhoc.api.v1.IIsExplainable;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.transformers.Aggregator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

/**
 * A Database query is dedicated to querying external database.
 * 
 * @author Benoit Lacelle
 * @see eu.solven.adhoc.database.IAdhocDatabaseTranscoder
 */
@Value
@AllArgsConstructor
@Builder
public class DatabaseQuery implements IWhereGroupbyAdhocQuery {

	@Default
	IAdhocFilter filter = IAdhocFilter.MATCH_ALL;

	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	// We query only simple aggregations to external databases
	@Default
	Set<Aggregator> aggregators = Collections.emptySet();

	@Default
	boolean debug = false;
	@Default
	boolean explain = false;

	public DatabaseQuery(IHasFilters hasFilter, IHasGroupBy groupBy, Set<Aggregator> aggregators) {
		this.filter = hasFilter.getFilter();
		this.groupBy = groupBy.getGroupBy();
		this.aggregators = aggregators;

		this.debug = false;
		this.explain = false;
	}

	public static DatabaseQueryBuilder edit(DatabaseQuery dq) {
		return DatabaseQuery.edit((IWhereGroupbyAdhocQuery) dq)
				.aggregators(dq.getAggregators())
				.debug(dq.isDebug())
				.explain(dq.isExplain());
	}

	public static DatabaseQueryBuilder edit(IWhereGroupbyAdhocQuery dq) {
		DatabaseQueryBuilder builder = DatabaseQuery.builder().filter(dq.getFilter()).groupBy(dq.getGroupBy());

		if (dq instanceof IIsDebugable isDebugable) {
			builder.debug(isDebugable.isDebug());
		}
		if (dq instanceof IIsExplainable isExplainable) {
			builder.explain(isExplainable.isExplain());
		}

		return builder;
	}
}