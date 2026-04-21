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
package eu.solven.adhoc.table.sql;

import org.jooq.TableLike;

import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqSnowflakeSchemaBuilder;

/**
 * Produces the jOOQ {@link TableLike} used in the {@code FROM} clause of a given {@link TableQueryV4}.
 * <p>
 * The default {@link JooqTableWrapper} path uses a single constant table supplied via
 * {@code JooqTableWrapperParameters.table}. Alternative implementations (e.g.
 * {@link PrunedJoinsJooqSnowflakeSchemaBuilder}) produce a query-specific {@link TableLike} — typically by pruning
 * joins that are not reachable from the columns actually referenced by the query.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IJooqTableSupplier {

	/**
	 * @param tableQuery
	 *            the query about to be prepared
	 * @return the {@link TableLike} to place in the {@code FROM} clause for this query
	 */
	TableLike<?> tableFor(TableQueryV4 tableQuery);

	/**
	 * Wraps a constant table as an {@link IJooqTableSupplier} that ignores the query.
	 */
	static IJooqTableSupplier constant(TableLike<?> table) {
		return _ -> table;
	}
}
