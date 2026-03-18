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

import java.util.List;
import java.util.stream.IntStream;

import org.jooq.Record;
import org.jooq.ResultQuery;

import lombok.Builder;

/**
 * Splits a query in multiple queries to take advantage of partitioning.
 *
 * BEWARE We need to evaluate when this is better than just letting ITableWrapper partition itself. If Adhoc has low
 * concurrency, it may help streaming results from table, instead of waiting for one large result.
 *
 *
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not functional")
@Builder
public class ModuloQueryPartitionor implements IQueryPartitionor {
	/**
	 * Example SQL to infer partitions from a PGSQL database
	 */
	static final String FETCH_PARTITION_KEYS = """
			SELECT
			    attname AS sortkey_column,
			    a.attsortkeyord AS sortkey_order
			FROM
			    pg_class c
			    JOIN pg_namespace n ON c.relnamespace = n.oid
			    JOIN pg_attribute a ON a.attrelid = c.oid
			WHERE
			    n.nspname = 'someSchema'
			    AND c.relname = 'someTableName'
			    AND a.attsortkeyord > 0
			ORDER BY a.attsortkeyord;
			""";

	private static final int DEFAULT_MODULO = 4;

	@Builder.Default
	public int modulo = DEFAULT_MODULO;

	@Override
	public List<ResultQuery<Record>> partition(ResultQuery<Record> query) {
		// `mod(abs(from_hex(substring(md5("someColumn"), 1, 16))::bigint), 4) = 0`
		return IntStream.range(0, modulo).mapToObj(index -> query).toList();
	}
}
