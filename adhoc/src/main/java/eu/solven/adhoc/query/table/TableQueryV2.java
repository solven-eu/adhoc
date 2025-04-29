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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.top.AdhocTopClause;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * A query over an {@link ITableWrapper}, which typically represents an external database.
 * 
 * This V2 tries to runs a smaller number of table queries, by grouping queries with the same groupBy, and relying on
 * `FILTER` clause for per-aggregator filtering, on-top of the `WHERE` clause.
 * 
 * @author Benoit Lacelle
 * @see eu.solven.adhoc.table.transcoder.ITableTranscoder
 */
@Value
@Builder(toBuilder = true)
@Deprecated(since = "Not-Ready")
// https://blog.jooq.org/how-to-calculate-multiple-aggregate-functions-in-a-single-query/
public class TableQueryV2
		implements IWhereGroupByQuery, IHasCustomMarker, IIsExplainable, IIsDebugable, IHasQueryOptions {

	// a filter shared through all aggregators
	@Default
	IAdhocFilter filter = IAdhocFilter.MATCH_ALL;

	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	// We query only simple aggregations to external databases
	@Singular
	@NonNull
	ImmutableSet<FilteredAggregator> aggregators;

	// This property is transported down to the TableQuery
	@Default
	Object customMarker = null;

	@Default
	AdhocTopClause topClause = AdhocTopClause.NO_LIMIT;

	@NonNull
	@Singular
	ImmutableSet<IQueryOption> options;

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

	public static TableQueryV2.TableQueryV2Builder edit(TableQuery tableQuery) {
		return TableQueryV2.builder()
				.groupBy(tableQuery.getGroupBy())
				.customMarker(tableQuery.getCustomMarker())
				.options(tableQuery.getOptions())
				.topClause(tableQuery.getTopClause());
	}

	public static Set<TableQueryV2> fromV1(Set<TableQuery> tableQueries) {
		Multimap<TableQuery, FilteredAggregator> groupByToFilteredAggregators =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

		AtomicLongMap<List<?>> filteredAggregatorAliasIndex = AtomicLongMap.create();

		tableQueries.forEach(tableQuery -> {
			if (tableQuery.getAggregators().isEmpty()) {
				TableQuery groupBy = TableQuery.edit(tableQuery)
						.clearAggregators()
						// remove the filter from the `WHERE`
						.filter(IAdhocFilter.MATCH_ALL)
						.build();
				FilteredAggregator filteredAggregator = FilteredAggregator.builder()
						// transfer the whole filter as `FILTER`
						.filter(tableQuery.getFilter())
						.aggregator(Aggregator.empty())
						.build();
				groupByToFilteredAggregators.put(groupBy, filteredAggregator);
			} else {
				tableQuery.getAggregators().forEach(aggregator -> {
					// use as groupBy the tableQuery without aggregators and filters
					// it enables keeping other options like customMarkers, which behave like groupBy in this phase
					TableQuery groupBy = TableQuery.edit(tableQuery)
							.clearAggregators()
							// remove the filter from the `WHERE`
							.filter(IAdhocFilter.MATCH_ALL)
							.build();
					FilteredAggregator filteredAggregator = FilteredAggregator.builder()
							.aggregator(aggregator)
							// transfer the whole filter as `FILTER`
							.filter(tableQuery.getFilter())
							// The index should be unique per groupBy+aggregator, but it is simpler and totally fine to
							// increment more often
							.index(filteredAggregatorAliasIndex.getAndIncrement(List.of(groupBy, aggregator.getName())))
							.build();
					groupByToFilteredAggregators.put(groupBy, filteredAggregator);
				});
			}
		});

		Set<TableQueryV2> queriesV2 = new LinkedHashSet<>();

		groupByToFilteredAggregators.asMap().forEach((groupBy, filteredAggregators) -> {
			// This is the filter applicable to all aggregators: it will be applied in WHERE
			Set<IAdhocFilter> filters = filteredAggregators.stream()
					.map(fa -> fa.getFilter())
					.collect(Collectors.toCollection(LinkedHashSet::new));
			IAdhocFilter commonFilter = commonFilter(filters);

			TableQueryV2Builder v2Builder = TableQueryV2.edit(groupBy).filter(commonFilter);

			filteredAggregators.forEach(filteredAggregator -> {
				IAdhocFilter strippedFromWhere = stripFilterFromWhere(commonFilter, filteredAggregator.getFilter());
				v2Builder.aggregator(filteredAggregator.toBuilder().filter(strippedFromWhere).build());
			});

			queriesV2.add(v2Builder.build());
		});

		return queriesV2;
	}

	public static TableQueryV2 fromV1(TableQuery tableQuery) {
		return Iterables.getOnlyElement(fromV1(Set.of(tableQuery)));
	}

	/**
	 * 
	 * @param where
	 *            some `WHERE` clause
	 * @param filter
	 *            some `FILTER` clause
	 * @return an equivalent `FILTER` clause, simplified given the `WHERE` clause.
	 */
	public static IAdhocFilter stripFilterFromWhere(IAdhocFilter where, IAdhocFilter filter) {
		if (where.isMatchAll()) {
			// `WHERE` has no clause: `FILTER` has to keep all clauses
			return filter;
		} else if (AndFilter.and(where, filter).equals(where)) {
			// Catch some edge-case like `where.equals(filter)`
			// More generally: if `WHERE && FILTER === WHERE`, then `FILTER` is irrelevant
			return IAdhocFilter.MATCH_ALL;
		}

		// Split the FILTER in smaller parts
		Set<? extends IAdhocFilter> andOperators = splitAnd(filter);

		Set<IAdhocFilter> notInWhere = new LinkedHashSet<IAdhocFilter>();

		// For each part of `FILTER`, reject those already filtered in `WHERE`
		for (IAdhocFilter subFilter : andOperators) {
			boolean whereCoversSubFilter = AndFilter.and(where, subFilter).equals(where);

			if (!whereCoversSubFilter) {
				notInWhere.add(subFilter);
			}
		}

		return AndFilter.and(notInWhere);
	}

	/**
	 * Split the filter in a Set of {@link IAdhocFilter}, equivalent by AND to the original filter.
	 * 
	 * @param filter
	 * @return
	 */
	protected static Set<IAdhocFilter> splitAnd(IAdhocFilter filter) {
		if (filter.isMatchAll() || filter.isMatchNone()) {
			return Set.of(filter);
		} else if (filter instanceof IAndFilter andFilter) {
			return andFilter.getOperands();
		} else if (filter instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();

			String column = columnFilter.getColumn();
			if (valueMatcher instanceof AndMatcher andMatcher) {
				return andMatcher.getOperands()
						.stream()
						.map(operand -> ColumnFilter.builder().column(column).valueMatcher(operand).build())
						.collect(Collectors.toCollection(LinkedHashSet::new));
			}

		}

		// Not splittable
		return Set.of(filter);
	}

	public static IAdhocFilter commonFilter(Set<? extends IAdhocFilter> filters) {
		if (filters.isEmpty()) {
			return IAdhocFilter.MATCH_ALL;
		} else if (filters.size() == 1) {
			return filters.iterator().next();
		}

		Iterator<? extends IAdhocFilter> iterator = filters.iterator();
		// Common parts are initialized with all parts of the first filter
		Set<IAdhocFilter> commonParts = new LinkedHashSet<>(splitAnd(iterator.next()));

		while (iterator.hasNext()) {
			Set<IAdhocFilter> nextFilterParts = new LinkedHashSet<>(splitAnd(iterator.next()));

			commonParts = Sets.intersection(commonParts, nextFilterParts);
		}

		return AndFilter.and(commonParts);
	}
}