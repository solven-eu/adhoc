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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
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
// https://blog.jooq.org/how-to-calculate-multiple-aggregate-functions-in-a-single-query/
public class TableQueryV2 implements IWhereGroupByQuery, IHasCustomMarker, IHasQueryOptions {

	// a filter shared through all aggregators
	@Default
	ISliceFilter filter = ISliceFilter.MATCH_ALL;

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

		// groupBy tableQueries by `GROUP BY`, as TableQueryV2 relies on a single `GROUP BY` and as many `FILTER` as
		// possible.
		tableQueries.forEach(tableQuery -> {
			tableQuery.getAggregators().forEach(aggregator -> {
				// use as groupBy the tableQuery without aggregators and filters
				// it enables keeping other options like customMarkers, which behave like groupBy in this phase
				TableQuery groupBy = TableQuery.edit(tableQuery)
						.clearAggregators()
						// remove the filter from the `WHERE`
						.filter(ISliceFilter.MATCH_ALL)
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
		});

		Set<TableQueryV2> queriesV2 = new LinkedHashSet<>();

		groupByToFilteredAggregators.asMap().forEach((groupBy, filteredAggregators) -> {
			// This is the filter applicable to all aggregators: it will be applied in WHERE
			Set<ISliceFilter> filters = filteredAggregators.stream()
					.map(FilteredAggregator::getFilter)
					.collect(Collectors.toCollection(LinkedHashSet::new));
			ISliceFilter commonFilter = FilterHelpers.commonFilter(filters);

			TableQueryV2Builder v2Builder = edit(groupBy).filter(commonFilter);

			filteredAggregators.forEach(filteredAggregator -> {
				ISliceFilter strippedFromWhere =
						FilterHelpers.stripWhereFromFilter(commonFilter, filteredAggregator.getFilter());
				v2Builder.aggregator(filteredAggregator.toBuilder().filter(strippedFromWhere).build());
			});

			queriesV2.add(v2Builder.build());
		});

		return queriesV2;
	}

	public static TableQueryV2 fromV1(TableQuery tableQuery) {
		return Iterables.getOnlyElement(fromV1(Set.of(tableQuery)));
	}
}