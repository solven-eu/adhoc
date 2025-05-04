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
package eu.solven.adhoc.table.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.dag.context.ExecutingQueryContext;
import eu.solven.adhoc.data.row.HideAggregatorsTabularRecord;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV2.TableQueryV2Builder;
import eu.solven.adhoc.table.ICustomMarkerCacheStrategy;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A decorating {@link ITableWrapper} which adds a cache layer.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class CachingTableWrapper implements ITableWrapper {

	@NonNull
	final ITableWrapper decorated;

	@Value
	public static class CachingKey {
		// Must refer to a single measure, not filtered
		TableQueryV2 tableQuery;

		@Builder
		public CachingKey(TableQueryV2 tableQuery) {
			this.tableQuery = tableQuery;

			if (tableQuery.getAggregators().isEmpty()) {
				// Request for slices/emptyMeasure
			} else {
				if (tableQuery.getAggregators().size() != 1) {
					throw new IllegalArgumentException("Must have a single aggregator. Was %s".formatted(tableQuery));
				}

				if (!IAdhocFilter.MATCH_ALL.equals(Iterables.getOnlyElement(tableQuery.getAggregators()).getFilter())) {
					throw new IllegalArgumentException("Aggregator must not be filtered. Was %s".formatted(tableQuery));
				}
			}
		}
	}

	@NonNull
	@Default
	final Cache<CachingKey, ImmutableList<ITabularRecord>> cache = CacheBuilder.newBuilder()
			.recordStats()
			// https://github.com/google/guava/issues/3202
			.<CachingKey, ImmutableList<ITabularRecord>>weigher((key, value) -> {
				// TODO Adjust with the weight of the aggregate
				return value.size() * key.getTableQuery().getGroupBy().getGroupedByColumns().size();
			})
			.maximumWeight(1024 * 1024)
			.build();

	public void invalidateAll() {
		cache.invalidateAll();
	}

	@Override
	public Collection<ColumnMetadata> getColumns() {
		return decorated.getColumns();
	}

	@Override
	public String getName() {
		// Do not prefix/edit the name, as we typically want this ITableWrapper to work in name of the underlying table
		return decorated.getName();
	}

	@Override
	public ITabularRecordStream streamSlices(ExecutingQueryContext executingQueryContext, TableQueryV2 tableQuery) {
		Map<FilteredAggregator, ImmutableList<ITabularRecord>> cached = new LinkedHashMap<>();
		List<FilteredAggregator> notCached = new ArrayList<>();

		Object customMarkerForCache;
		ITableWrapper table = executingQueryContext.getTable();
		if (table instanceof ICustomMarkerCacheStrategy cacheStrategy) {
			customMarkerForCache = cacheStrategy.restrictToCacheImpact(tableQuery.getCustomMarker());
		} else {
			// If the table does not implement ICustomMarkerCacheStrategy, it means customMarker are not playing any
			// role in table query logic
			customMarkerForCache = null;
		}

		// Split the queried aggregators if available from cache or not
		for (FilteredAggregator aggregator : tableQuery.getAggregators()) {
			TableQueryV2 queryForCache = tableQuery.toBuilder()
					.clearAggregators()
					.aggregator(FilteredAggregator.builder().aggregator(aggregator.getAggregator()).build())
					.filter(AndFilter.and(tableQuery.getFilter(), aggregator.getFilter()))
					.customMarker(customMarkerForCache)
					.build();

			CachingKey cacheKey = CachingKey.builder().tableQuery(queryForCache).build();

			ImmutableList<ITabularRecord> nullableCacheResult = cache.getIfPresent(cacheKey);
			if (nullableCacheResult == null) {
				notCached.add(aggregator);
			} else {
				cached.put(aggregator, nullableCacheResult);
			}
		}

		if (notCached.isEmpty()) {
			// All aggregators are available from cache
			return new ITabularRecordStream() {

				@Override
				public Stream<ITabularRecord> records() {
					return mergeAggregates(cached);
				}

				@Override
				public void close() {
					// nothing to close as read from cache
				}
			};
		} else {
			TableQueryV2 queryAgrgegatorsNotCached = querySubset(tableQuery, notCached);

			ITabularRecordStream decoratedRecordsStream =
					streamDecorated(executingQueryContext, queryAgrgegatorsNotCached);

			return new ITabularRecordStream() {

				@Override
				public Stream<ITabularRecord> records() {
					List<ITabularRecord> columns = decoratedRecordsStream.records().toList();

					decoratedRecordsStream.close();

					log.info("Done receiving tabularRecords for t={} q={}",
							executingQueryContext.getTable().getName(),
							tableQuery);

					Map<FilteredAggregator, List<ITabularRecord>> cachedAndJustInTime = new LinkedHashMap<>();

					cachedAndJustInTime.putAll(cached);

					// add the missing aggregates into cache
					for (FilteredAggregator aggregator : queryAgrgegatorsNotCached.getAggregators()) {
						TableQueryV2 queryForCache = tableQuery.toBuilder()
								.clearAggregators()
								.aggregator(FilteredAggregator.builder().aggregator(aggregator.getAggregator()).build())
								.filter(AndFilter.and(tableQuery.getFilter(), aggregator.getFilter()))
								.customMarker(customMarkerForCache)
								.build();

						CachingKey cacheKey = CachingKey.builder().tableQuery(queryForCache).build();

						ImmutableList<ITabularRecord> column = columns.stream()
								.map(r -> enableSingleAggregate(r, aggregator.getAlias()))
								.collect(ImmutableList.toImmutableList());
						cache.put(cacheKey, column);
						cachedAndJustInTime.put(aggregator, column);
					}

					if (cached.isEmpty()) {
						// Skip the merging process to escape the merging penalty
						// But there is a risk of having different results
						return columns.stream();
					} else {
						return mergeAggregates(cachedAndJustInTime);
					}
				}

				@Override
				public void close() {
					// nothing to close as either read from cache, or closed when reading decorated
				}

				protected ITabularRecord enableSingleAggregate(ITabularRecord r, String aggregate) {
					return HideAggregatorsTabularRecord.builder().decorated(r).keptAggregate(aggregate).build();
				}

			};
		}

	}

	protected Stream<ITabularRecord> mergeAggregates(Map<FilteredAggregator, ? extends List<ITabularRecord>> cached) {
		// TODO Resolve aliases
		return cached.values().stream().flatMap(s -> s.stream());

		// Map<Map<String, ?>, Map<String, ?>> sliceToAggregates = new LinkedHashMap<>();
		//
		// cached.forEach((aggregator, column) -> {
		// column.forEach(record -> {
		// sliceToAggregates
		// .merge(record.getGroupBys(), record.aggregatesAsMap(), (aggregatesLeft, aggregatesRight) -> {
		// Map<String, Object> merged = new LinkedHashMap<>();
		//
		// merged.putAll(aggregatesLeft);
		// merged.putAll(aggregatesRight);
		//
		// return merged;
		// });
		// });
		// });
		//
		// return sliceToAggregates.entrySet()
		// .stream()
		// .map(e -> TabularRecordOverMaps.builder().slice(e.getKey()).aggregates(e.getValue()).build());
	}

	protected TableQueryV2 querySubset(TableQueryV2 tableQuery, List<FilteredAggregator> notCached) {
		// TODO We could move some common filter into the query filter. Would it bring any advantage?
		return tableQuery.toBuilder().clearAggregators().aggregators(notCached).build();
	}

	protected CachingKey makeCacheKey(ExecutingQueryContext executingQueryContext, TableQueryV2 tableQuery) {
		TableQueryV2Builder queryKeyForCache = tableQuery.toBuilder();

		ITableWrapper table = executingQueryContext.getTable();
		if (table instanceof ICustomMarkerCacheStrategy cacheStrategy) {
			Object customMarkerForCache = cacheStrategy.restrictToCacheImpact(tableQuery.getCustomMarker());
			queryKeyForCache.customMarker(customMarkerForCache);
		} else {
			// If the table does not implement ICustomMarkerCacheStrategy, it means customMarker are not playing any
			// role in table query logic
			queryKeyForCache.customMarker(null);
		}

		return CachingKey.builder().tableQuery(queryKeyForCache.build()).build();
	}

	protected ITabularRecordStream streamDecorated(ExecutingQueryContext executingQueryContext,
			TableQueryV2 tableQuery) {
		ExecutingQueryContext decoratedContext = executingQueryContext.toBuilder().table(decorated).build();
		return decorated.streamSlices(decoratedContext, tableQuery);
	}

	protected Optional<List<ITabularRecord>> resultFromCache(CachingKey cacheKey) {
		return Optional.ofNullable(cache.getIfPresent(cacheKey));
	}

	/**
	 * 
	 * @return a snapshot of {@link CacheStats} associated to the cache. These statistics are on a per-aggregator basis.
	 */
	public CacheStats getCacheStats() {
		return cache.stats();
	}
}
