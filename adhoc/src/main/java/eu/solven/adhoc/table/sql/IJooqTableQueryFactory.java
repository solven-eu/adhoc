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

import java.util.List;

import org.jooq.Record;
import org.jooq.ResultQuery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.cleanthat.SuppressCleanthat;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 * Converts a {@link TableQuery} into a sql {@link ResultQuery}
 * 
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IJooqTableQueryFactory {
	/**
	 * The result of splitting an {@link TableQueryV2} into a leg executable by the SQL database, and a filter to be
	 * applied manually over the output from the database.
	 *
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	// For some reason, cleanthat is modifying this class
	@SuppressCleanthat
	class QueryWithLeftover {
		// May have multiple queries (e.g. if partitioning modulo some partitionKey)
		private final List<ResultQuery<Record>> queries;

		/**
		 * a filter to apply over the results from the SQL engine. Typically used for custom {@link ISliceFilter}, which
		 * can not be translated into the SQL engine.
		 */
		ISliceFilter leftover;

		@Singular
		ImmutableMap<String, ISliceFilter> aggregatorToLeftovers;

		AggregatedRecordFields fields;

		@Deprecated(since = "This may fail if the query has been partitioned. Might be fine for unitTests.")
		public ResultQuery<Record> getQuery() {
			return Iterables.getOnlyElement(queries);
		}
	}

	QueryWithLeftover prepareQuery(TableQueryV3 tableQuery);

}
