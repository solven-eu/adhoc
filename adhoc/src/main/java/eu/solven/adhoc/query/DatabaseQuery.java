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

import java.util.Set;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IHasCustomMarker;
import eu.solven.adhoc.api.v1.IIsDebugable;
import eu.solven.adhoc.api.v1.IIsExplainable;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.transformers.Aggregator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Singular;
import lombok.Value;

/**
 * A Database query is dedicated to querying external database.
 * 
 * @author Benoit Lacelle
 * @see eu.solven.adhoc.database.transcoder.IAdhocDatabaseTranscoder
 */
@Value
@AllArgsConstructor
@Builder
public class DatabaseQuery implements IWhereGroupbyAdhocQuery, IHasCustomMarker {

	@Default
	IAdhocFilter filter = IAdhocFilter.MATCH_ALL;

	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	// We query only simple aggregations to external databases
	@Singular
	Set<Aggregator> aggregators;

	// This property is transported down to the DatabaseQuery
	@Default
	Object customMarker = null;

	@Default
	AdhocTopClause topClause = AdhocTopClause.NO_LIMIT;

	@Default
	boolean debug = false;
	@Default
	boolean explain = false;

	public static DatabaseQueryBuilder edit(DatabaseQuery dq) {
		return edit((IWhereGroupbyAdhocQuery) dq).aggregators(dq.getAggregators())
				.debug(dq.isDebug())
				.explain(dq.isExplain());
	}

	public static DatabaseQueryBuilder edit(IWhereGroupbyAdhocQuery dq) {
		DatabaseQueryBuilder builder = DatabaseQuery.builder().filter(dq.getFilter()).groupBy(dq.getGroupBy());

		if (dq instanceof IHasCustomMarker hasCustomMarker) {
			hasCustomMarker.optCustomMarker().ifPresent(builder::customMarker);
		}
		if (dq instanceof IIsDebugable isDebugable) {
			builder.debug(isDebugable.isDebug());
		}
		if (dq instanceof IIsExplainable isExplainable) {
			builder.explain(isExplainable.isExplain());
		}

		return builder;
	}
}