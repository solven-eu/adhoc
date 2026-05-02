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

import org.jooq.SQLDialect;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

/**
 * Helps {@link JooqTableQueryFactory} crafting SQLs, given capabilities of jOOQ.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class JooqTableCapabilities {

	// Typically used for RedShift when it is queried with PostgreSQL dialect
	@Default
	boolean ableToGroupByAll = false;

	// Typically used for RedShift when it is queried with PostgreSQL dialect
	// If true, one can `SUM(v) FILTER (c > 123)`
	// If false, we typically fallback on `CASE`
	// https://modern-sql.com/feature/filter
	@Default
	boolean ableToFilterAggregates = false;

	@Deprecated(since = "Should we rather rely on JooQ Commercial versions?")
	public static JooqTableCapabilities from(SQLDialect dialect) {
		JooqTableCapabilitiesBuilder builder = JooqTableCapabilities.builder();

		// https://duckdb.org/docs/stable/sql/query_syntax/groupby.html#group-by-all
		// https://docs.snowflake.com/en/sql-reference/constructs/group-by
		if (dialect == SQLDialect.DUCKDB) {
			builder.ableToGroupByAll(true);
			builder.ableToFilterAggregates(true);
		} else if (dialect == SQLDialect.POSTGRES) {
			// BEWARE This is false for RedShift, which dialect is similar to PostgreSQL
			builder.ableToFilterAggregates(true);
		} else if (dialect == SQLDialect.SQLITE) {
			builder.ableToFilterAggregates(true);
		}

		return builder.build();
	}

}
