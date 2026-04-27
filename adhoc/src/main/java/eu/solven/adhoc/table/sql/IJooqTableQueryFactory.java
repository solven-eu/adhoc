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
package eu.solven.adhoc.table.sql;

import org.jooq.ResultQuery;

import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;

/**
 * Converts a {@link TableQuery} into a sql {@link ResultQuery}
 * 
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IJooqTableQueryFactory {

	QueryWithLeftover prepareSliceQuery(TableQueryV4 tableQuery);

	/**
	 * Build a non-aggregating SQL query for the {@link eu.solven.adhoc.options.StandardQueryOptions#DRILLTHROUGH} path:
	 * one record per matched row, no GROUP BY, per-aggregator FILTER encoded as a {@code CASE WHEN} on the column.
	 *
	 * <p>
	 * Default implementation delegates to {@link #prepareSliceQuery(TableQueryV4)}, preserving the legacy GROUP BY
	 * behavior for factories that do not specialize the path.
	 */
	default QueryWithLeftover prepareRowsQuery(TableQueryV3 tableQuery) {
		return prepareSliceQuery(tableQuery.toV4());
	}

}
