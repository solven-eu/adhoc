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
package eu.solven.adhoc.query.table;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.top.AdhocTopClause;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.AggregatedRecordFields;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * A query over an {@link ITableWrapper}, which typically represents an external database.
 * 
 * @author Benoit Lacelle
 * @see eu.solven.adhoc.table.transcoder.ITableTranscoder
 */
@Value
@Builder
public class TableQuery
		implements IWhereGroupByQuery, IHasCustomMarker, IIsExplainable, IIsDebugable, IHasQueryOptions {

	@Default
	IAdhocFilter filter = IAdhocFilter.MATCH_ALL;

	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	// We query only simple aggregations to external databases
	@Singular
	@NonNull
	ImmutableSet<Aggregator> aggregators;

	// This property is transported down to the TableQuery
	@Default
	Object customMarker = null;

	@Default
	AdhocTopClause topClause = AdhocTopClause.NO_LIMIT;

	@NonNull
	@Singular
	ImmutableSet<IQueryOption> options;

	public static TableQueryBuilder edit(TableQuery tableQuery) {
		return edit((IWhereGroupByQuery) tableQuery).aggregators(tableQuery.getAggregators());
	}

	public static TableQueryBuilder edit(IWhereGroupByQuery query) {
		TableQueryBuilder builder = TableQuery.builder().filter(query.getFilter()).groupBy(query.getGroupBy());

		if (query instanceof IHasCustomMarker hasCustomMarker) {
			hasCustomMarker.optCustomMarker().ifPresent(builder::customMarker);
		}
		if (query instanceof IIsExplainable isExplainable && isExplainable.isExplain()) {
			builder.option(StandardQueryOptions.EXPLAIN);
		}
		if (query instanceof IIsDebugable isDebugable && isDebugable.isDebug()) {
			builder.option(StandardQueryOptions.DEBUG);
		}
		if (query instanceof IHasQueryOptions hasQueryOptions) {
			builder.options(hasQueryOptions.getOptions());
		}

		return builder;
	}

	/**
	 * @param tableQuery
	 *            the initial tableQuery
	 * @param leftover
	 *            the filter which has to be applied manually over the output slices (e.g. on a customFilter which can
	 *            not be transcoded for given table)
	 * @return the {@link List} of the columns to be output by the tableQuery
	 */
	// BEWARE Is this a JooQ specific logic?
	public static AggregatedRecordFields makeSelectedColumns(TableQuery tableQuery, IAdhocFilter leftover) {
		List<String> aggregatorNames = new ArrayList<>();
		tableQuery.getAggregators()
				.stream()
				.distinct()
				.filter(a -> !EmptyAggregation.isEmpty(a.getAggregationKey()))
				.forEach(a -> aggregatorNames.add(a.getName()));

		List<String> columns = new ArrayList<>();
		tableQuery.getGroupBy().getNameToColumn().values().forEach(column -> {
			columns.add(column.getName());
		});

		List<String> lateColumns = new ArrayList<>();
		FilterHelpers.getFilteredColumns(leftover).forEach(lateColumn -> {
			lateColumns.add(lateColumn);
		});

		// Make sure a latecolumn is not also a normal groupBy column
		lateColumns.removeAll(columns);

		return AggregatedRecordFields.builder()
				.aggregates(aggregatorNames)
				.columns(columns)
				.lateColumns(lateColumns)
				.build();
	}

	@Deprecated(since = "Use .getOptions()")
	@Override
	public boolean isDebug() {
		return options.contains(StandardQueryOptions.DEBUG);
	}

	@Deprecated(since = "Use .getOptions()")
	@Override
	public boolean isExplain() {
		return options.contains(StandardQueryOptions.EXPLAIN);
	}
}