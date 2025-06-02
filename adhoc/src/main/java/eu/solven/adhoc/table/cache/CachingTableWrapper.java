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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.data.row.HideAggregatorsTabularRecord;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.query.StandardQueryOptions;
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
 * Performance (CPU) trade-off: this caching is efficient if the table is slow to return a low number of slices. But it
 * can be less efficient than external table in merging different columns. Given 2 different cached columns, these has
 * to be merged (at some point in the engine) in order to associate the 2 columns aggregates to the common slices.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
@Deprecated(since = "May need time to stabilize")
public class CachingTableWrapper implements ITableWrapper {
	// ~1GB
	private static final int DEFAULT_MAX_WEIGHT = 1024 * 1024 * 1024;

	@NonNull
	final ITableWrapper decorated;

	@NonNull
	@Default
	final Cache<CachingKey, CachingValue> cache = defaultCacheBuilder().maximumWeight(DEFAULT_MAX_WEIGHT).build();

	/**
	 * Key for the cache. It refers to a single non-filtering {@link FilteredAggregator}.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	public static class CachingKey {
		// Must refer to a single measure, not filtered
		TableQueryV2 tableQuery;

		@Builder
		public CachingKey(TableQueryV2 tableQuery) {
			this.tableQuery = tableQuery;

			if (tableQuery.getAggregators().isEmpty()) {
				log.trace("Request for slices/emptyMeasure");
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

	/**
	 * Value for the cache. The cache is per-Aggregator. But the received-values are typically for a {@link Set} of
	 * aggregators: we want to be able to return a multi-aggregate {@link ITabularRecord}, instead or rebuilding them
	 * manually. Once important consideration is performance, as it enables skipping many groupBy-slice, which would be
	 * necessary if we return per-aggregator independent columns.
	 * 
	 * TODO The Cache discarding policy is awkward, as we weight each aggregator individually while the cache value
	 * refers to the whole original Set of aggregators. ANother related issue is than the Cache may discard some
	 * aggregators while they are implicitly still referred, so they should remain usable.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class CachingValue {
		@NonNull
		List<CachingKey> aggregators;

		// Must refer to a single measure, not filtered
		@NonNull
		ImmutableList<ITabularRecord> records;
	}

	public static CacheBuilder<CachingKey, CachingValue> defaultCacheBuilder() {
		return CacheBuilder.newBuilder()
				.recordStats()
				// https://github.com/google/guava/issues/3202
				.<CachingKey, CachingValue>weigher((key, value) -> {
					// TODO Adjust with the weight of the aggregate
					return value.getRecords().size() * key.getTableQuery().getGroupBy().getGroupedByColumns().size();
				})
				// Do not set a maximum weight else it can not be customized (as per Guava constrain)
				// .maximumWeight(1024 * 1024)
				.removalListener(new RemovalListener<>() {
					@Override
					public void onRemoval(RemovalNotification<CachingKey, CachingValue> notification) {
						log.debug("RemovalNotification cause={} {} size={}",
								notification.getCause(),
								notification.getKey(),
								notification.getValue().getRecords().size());
					}
				});
	}

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

	@SuppressWarnings({ "PMD.NullAssignment", "PMD.CloseResource" })
	@Override
	public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV2 tableQuery) {
		if (queryPod.getOptions().contains(StandardQueryOptions.NO_CACHE)) {
			return streamDecorated(queryPod, tableQuery);
		}

		Map<FilteredAggregator, CachingValue> fromCache = new LinkedHashMap<>();
		List<FilteredAggregator> notCached = new ArrayList<>();

		Object customMarkerForCache;
		ITableWrapper table = queryPod.getTable();
		if (table instanceof ICustomMarkerCacheStrategy cacheStrategy) {
			customMarkerForCache = cacheStrategy.restrictToCacheImpact(tableQuery.getCustomMarker());
		} else {
			// If the table does not implement ICustomMarkerCacheStrategy, it means customMarker are not playing any
			// role in table query logic
			customMarkerForCache = null;
		}

		// Split the queried aggregators if available from cache or not
		for (FilteredAggregator aggregator : tableQuery.getAggregators()) {
			CachingKey cacheKey = makeCacheKey(tableQuery, customMarkerForCache, aggregator);

			CachingValue nullableCacheResult = cache.getIfPresent(cacheKey);
			if (nullableCacheResult == null) {
				if (queryPod.isExplain()) {
					log.info("[EXPLAIN] cacheMiss on {}", aggregator);
				}
				notCached.add(aggregator);
			} else {
				if (queryPod.isExplain()) {
					log.info("[EXPLAIN] cacheHit on {}", aggregator);
				}
				fromCache.put(aggregator, nullableCacheResult);
			}
		}

		if (notCached.isEmpty()) {
			// All aggregators are available from cache
			return new ITabularRecordStream() {

				@Override
				public boolean isDistinctSlices() {
					// TODO `mergeAggregate` does not distinct slices
					return false;
				}

				@Override
				public Stream<ITabularRecord> records() {
					return mergeAggregates(tableQuery, customMarkerForCache, fromCache);
				}

				@Override
				public void close() {
					// nothing to close as read from cache
				}
			};
		} else {
			TableQueryV2 queryAggregatorsNotCached = querySubset(tableQuery, notCached);

			ITabularRecordStream decoratedRecordsStream = streamDecorated(queryPod, queryAggregatorsNotCached);

			// If the cache is empty, we'll return the result from underlying, which tells if distinct or not
			// If the cache is not empty, the merging process does not distinct slices, else it may generate duplicates
			boolean distinctSlices;
			if (fromCache.isEmpty()) {
				distinctSlices = decoratedRecordsStream.isDistinctSlices();
			} else {
				distinctSlices = false;
			}

			return new ITabularRecordStream() {

				@Override
				public boolean isDistinctSlices() {
					return distinctSlices;
				}

				@Override
				public Stream<ITabularRecord> records() {
					ImmutableList<ITabularRecord> columns =
							decoratedRecordsStream.records().collect(ImmutableList.toImmutableList());

					decoratedRecordsStream.close();

					log.debug("Done receiving tabularRecords for t={} q={}", queryPod.getTable().getName(), tableQuery);

					Map<FilteredAggregator, CachingValue> cachedAndJustInTime =
							new LinkedHashMap<>(tableQuery.getAggregators().size());

					cachedAndJustInTime.putAll(fromCache);

					List<CachingKey> notCachesKeys =
							notCached.stream().map(fa -> makeCacheKey(tableQuery, customMarkerForCache, fa)).toList();

					CachingValue cachingValue =
							CachingValue.builder().aggregators(notCachesKeys).records(columns).build();

					// add the missing aggregates into cache
					for (FilteredAggregator aggregator : queryAggregatorsNotCached.getAggregators()) {
						CachingKey cacheKey = makeCacheKey(tableQuery, customMarkerForCache, aggregator);

						cache.put(cacheKey, cachingValue);
						cachedAndJustInTime.put(aggregator, cachingValue);
					}

					if (fromCache.isEmpty()) {
						// Skip the merging process to escape the merging penalty
						// But there is a risk of having different results
						return columns.stream();
					} else {
						return mergeAggregates(tableQuery, customMarkerForCache, cachedAndJustInTime);
					}
				}

				@Override
				public void close() {
					// nothing to close as either read from cache, or closed when reading decorated
				}

			};
		}

	}

	private CachingKey makeCacheKey(TableQueryV2 tableQuery,
			Object customMarkerForCache,
			FilteredAggregator aggregator) {
		TableQueryV2 queryForCache = tableQuery.toBuilder()
				.clearAggregators()
				// TODO Should remove the alias from the cacheKey
				.aggregator(aggregator.toBuilder().filter(IAdhocFilter.MATCH_ALL).build())
				.filter(AndFilter.and(tableQuery.getFilter(), aggregator.getFilter()))
				.customMarker(customMarkerForCache)
				.build();

		return CachingKey.builder().tableQuery(queryForCache).build();
	}

	/**
	 * The goal here is to return {@link ITabularRecord} as wide as possible, to reduce the need for groupBy-slices in
	 * later steps. To do so, given the {@link Set} of {@link CachingValue}, we look for the most optimal
	 * {@link ITabularRecord} (given these may answer multiple aggregates).
	 * 
	 * @param tableQuery
	 * @param customMarkerForCache
	 * @param cached
	 * @return
	 */
	protected Stream<ITabularRecord> mergeAggregates(TableQueryV2 tableQuery,
			Object customMarkerForCache,
			Map<FilteredAggregator, ? extends CachingValue> cached) {
		// The list of CachingKey for which we're looking for a column
		List<CachingKey> neededCacheKeys = new ArrayList<>(
				cached.keySet().stream().map(fa -> makeCacheKey(tableQuery, customMarkerForCache, fa)).toList());

		// each `partitions` holds a Set of columns which are already grouped in the same record
		List<Stream<ITabularRecord>> partitions = new ArrayList<>();

		while (!neededCacheKeys.isEmpty()) {
			// Search the CachingValue answering the maximum number of cachingKeys
			Optional<? extends Map.Entry<FilteredAggregator, ? extends CachingValue>> max =
					cached.entrySet().stream().max(Comparator.comparing(e -> {
						return neededCacheKeys.stream()
								.filter(neededKey -> e.getValue().getAggregators().contains(neededKey))
								.count();
					}));

			if (max.isEmpty()) {
				throw new IllegalStateException("Nothing matching %s".formatted(neededCacheKeys));
			}

			CachingValue value = max.get().getValue();

			Set<String> aliasesToKeep = new LinkedHashSet<>();

			List<CachingKey> toRemove = new ArrayList<>();
			for (CachingKey neededKey : neededCacheKeys) {
				int index = value.getAggregators().indexOf(neededKey);

				if (index >= 0) {
					aliasesToKeep.add(neededKey.getTableQuery().getAggregators().iterator().next().getAlias());

					toRemove.add(neededKey);
				}
			}

			neededCacheKeys.removeAll(toRemove);

			// Guava enable fast `.copyOf`
			ImmutableSet<String> aliasesToKeepImmutable = ImmutableSet.copyOf(aliasesToKeep);

			ImmutableList<ITabularRecord> column = value.getRecords()
					.stream()
					.map(r -> enableSingleAggregate(r, aliasesToKeepImmutable))
					.collect(ImmutableList.toImmutableList());
			partitions.add(column.stream());
		}

		// TODO Resolve aliases
		return partitions.stream().flatMap(s -> s);
	}

	protected ITabularRecord enableSingleAggregate(ITabularRecord r, Set<String> aggregates) {
		return HideAggregatorsTabularRecord.builder()
				.decorated(r)
				.keptAggregates(ImmutableSet.copyOf(aggregates))
				.build();
	}

	protected TableQueryV2 querySubset(TableQueryV2 tableQuery, List<FilteredAggregator> notCached) {
		// TODO We could move some common filter into the query filter. Would it bring any advantage?
		return tableQuery.toBuilder().clearAggregators().aggregators(notCached).build();
	}

	protected CachingKey makeCacheKey(QueryPod queryPod, TableQueryV2 tableQuery) {
		TableQueryV2Builder queryKeyForCache = tableQuery.toBuilder();

		ITableWrapper table = queryPod.getTable();
		if (table instanceof ICustomMarkerCacheStrategy cacheStrategy) {
			Object customMarkerForCache = cacheStrategy.restrictToCacheImpact(tableQuery.getCustomMarker());
			queryKeyForCache.customMarker(customMarkerForCache);
		} else {
			// If the table does not implement ICustomMarkerCacheStrategy, it means customMarker are not playing any
			// role in table query logic: remove the customMarker from the cacheKey
			queryKeyForCache.customMarker(null);
		}

		return CachingKey.builder().tableQuery(queryKeyForCache.build()).build();
	}

	protected ITabularRecordStream streamDecorated(QueryPod queryPod, TableQueryV2 tableQuery) {
		QueryPod decoratedContext = queryPod.toBuilder().table(decorated).build();
		return decorated.streamSlices(decoratedContext, tableQuery);
	}

	/**
	 * 
	 * @return a snapshot of {@link CacheStats} associated to the cache. These statistics are on a per-aggregator basis.
	 */
	public CacheStats getCacheStats() {
		return cache.stats();
	}
}
