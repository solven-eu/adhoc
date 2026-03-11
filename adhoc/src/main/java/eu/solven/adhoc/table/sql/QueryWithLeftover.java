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
import java.util.Set;
import java.util.function.Predicate;

import org.jooq.Record;
import org.jooq.ResultQuery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.cleanthat.SuppressCleanthat;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 * The result of splitting an {@link TableQueryV2} into a leg executable by the SQL database, and a filter to be applied
 * manually over the output from the database.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
// For some reason, cleanthat is modifying this class
@SuppressCleanthat
public class QueryWithLeftover {
	// May have multiple queries (e.g. if partitioning modulo some partitionKey)
	private final List<ResultQuery<Record>> queries;

	/**
	 * a filter to apply over the results from the SQL engine. Typically used for custom {@link ISliceFilter}, which can
	 * not be translated into the SQL engine.
	 */
	ISliceFilter leftover;

	@Singular
	ImmutableMap<String, ISliceFilter> aggregatorToLeftovers;

	AggregatedRecordFields fields;

	// @Singular
	// ImmutableSet<Field<?>> groupingColumns;

	@Deprecated(since = "This may fail if the query has been partitioned. Might be fine for unitTests.")
	public ResultQuery<Record> getQuery() {
		return Iterables.getOnlyElement(queries);
	}

	/**
	 * @param tableQuery
	 *            the initial tableQuery
	 * @param leftovers
	 *            the filter which has to be applied manually over the output slices (e.g. on a customFilter which can
	 *            not be transcoded for given table). As a set as there may be a leftover on the common `WHERE` clause,
	 *            and on each `FILTER` clause.
	 * @return the {@link List} of the columns to be output by the tableQuery
	 */
	// BEWARE Is this a JooQ specific logic?
	public static AggregatedRecordFields makeSelectedColumns(TableQueryV3 tableQuery, Set<ISliceFilter> leftovers) {
		List<String> aggregatorNames = tableQuery.getAggregators()
				.stream()
				.distinct()
				.filter(a -> !EmptyAggregation.isEmpty(a.getAggregator().getAggregationKey()))
				.map(FilteredAggregator::getAlias)
				.toList();

		Set<String> groupByColumns = tableQuery.getGroupedByColumns();

		// Multiple `GROUP BY` -> `GROUPING SET`
		// TODO We could minimize the relevant `grouping` to columns which are not always present
		// https://github.com/jOOQ/jOOQ/issues/16465
		ImmutableSet<String> groupingColumns;
		if (tableQuery.singleGroupBy().isPresent()) {
			groupingColumns = ImmutableSet.of();
		} else {
			groupingColumns = groupByColumns.stream()
					.filter(c -> tableQuery.getGroupBys()
							.stream()
							.anyMatch(gb -> !gb.getGroupedByColumns().contains(c)))
					.collect(ImmutableSet.toImmutableSet());

		}

		List<String> leftoversColumns = leftovers.stream()
				.flatMap(leftover -> FilterHelpers.getFilteredColumns(leftover).stream())
				// Make sure a latecolumn is not also a normal groupBy column
				.filter(Predicate.not(groupByColumns::contains))
				.toList();

		return AggregatedRecordFields.builder()
				.aggregates(aggregatorNames)
				.columns(groupByColumns)
				.leftovers(leftoversColumns)
				.groupingColumns(groupingColumns)
				.build();
	}
}