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
package eu.solven.adhoc.beta.schema;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.column.ColumnMetadata.ColumnMetadataBuilder;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.MoreFilterHelpers;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManagerSimple;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.IHasName;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps together a Set of {@link ITableWrapper}, {@link IMeasureForest}, {@link ICubeWrapper} and {@link ICubeQuery}.
 * It is typically used for use through an API/Catalog.
 *
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
@SuppressWarnings("PMD.CouplingBetweenObjects")
public class AdhocSchema implements IAdhocSchema {
	@Builder.Default
	@NonNull
	final ICubeQueryEngine engine = CubeQueryEngine.builder().build();

	final Map<String, ITableWrapper> nameToTable = new ConcurrentHashMap<>();

	final Map<String, IMeasureForest> nameToForest = new ConcurrentHashMap<>();

	final Map<String, ICubeWrapper> nameToCube = new ConcurrentHashMap<>();

	final List<Map.Entry<CustomMarkerMatchingKey, CustomMarkerMetadataGenerator>> nameToCustomMarker =
			new CopyOnWriteArrayList<>();

	// final Map<String, IAdhocQuery> nameToQuery = new ConcurrentHashMap<>();

	// `getColumns` is an expensive operations, as it analyzes the underlying table
	final LoadingCache<String, Collection<? extends ColumnMetadata>> cacheCubeToColumnToType =
			CacheBuilder.newBuilder().expireAfterWrite(Duration.ofHours(1)).build(CacheLoader.from(cubeName -> {
				ICubeWrapper cube = nameToCube.get(cubeName);

				if (cube == null) {
					return List.<ColumnMetadata>of();
				} else {
					Collection<ColumnMetadata> rawColumns = cube.getColumns();

					ColumnIdentifier columnIdTemplate = ColumnIdentifier.builder()
							.isCubeElseTable(true)
							.holder(cubeName)
							.column("netYetDefined")
							.build();

					List<ColumnMetadata> enriched =
							rawColumns.stream().map(c -> enrichColumn(columnIdTemplate, c)).toList();
					return enriched;
				}
			}));

	// AdhocSchema may add additional tags to those hardcoded into the ICubeWrapper
	final Map<ColumnIdentifier, Set<String>> columnToTags = new ConcurrentHashMap<>();
	final Map<MeasureIdentifier, Set<String>> measureToTags = new ConcurrentHashMap<>();

	/**
	 * Used as key to identify in which context/cube given customMarker is relevant.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class CustomMarkerMatchingKey {
		/**
		 * Some name for the customMarker
		 */
		@NonNull
		String name;
		@NonNull
		IValueMatcher cubeMatcher;
	}

	protected ColumnMetadata enrichColumn(ColumnIdentifier columnIdTemplate, ColumnMetadata column) {
		ColumnMetadataBuilder builder = column.toBuilder();

		Set<String> additionalTags = columnToTags.get(columnIdTemplate.toBuilder().column(column.getName()).build());
		if (additionalTags != null) {
			builder.tags(additionalTags);
		}

		return builder.build();
	}

	protected IMeasure enrichMeasure(String cube, IMeasure value) {
		Set<String> additionalTags =
				measureToTags.get(MeasureIdentifier.builder().cube(cube).measure(value.getName()).build());

		if (additionalTags == null) {
			return value;
		}
		return value.withTags(ImmutableSet.copyOf(Sets.union(value.getTags(), additionalTags)));
	}

	public void invalidateAll() {
		cacheCubeToColumnToType.invalidateAll();
	}

	public CubeWrapper registerCube(String cubeName, String tableName, String forestName) {
		ITableWrapper table = nameToTable.get(tableName);
		if (table == null) {
			throw new IllegalArgumentException(
					"No table named %s amongst %s".formatted(tableName, nameToTable.keySet()));
		}

		IMeasureForest forest = nameToForest.get(forestName);
		if (forest == null) {
			throw new IllegalArgumentException(
					"No forest named %s amongst %s".formatted(forestName, nameToForest.keySet()));
		}
		CubeWrapper cube = CubeWrapper.builder().name(cubeName).engine(engine).table(table).forest(forest).build();

		nameToCube.put(cubeName, cube);

		return cube;
	}

	@Override
	public EndpointSchemaMetadata getMetadata(AdhocSchemaQuery query, boolean allIfEmpty) {
		EndpointSchemaMetadata.EndpointSchemaMetadataBuilder metadata = EndpointSchemaMetadata.builder();

		// A subset of the schema is requested: restrict ourselves to what's requested
		// Typically, we do not want to return a single cube, and all tables
		boolean hasAnyFilter =
				query.getCube().isPresent() || query.getForest().isPresent() || query.getTable().isPresent();

		nameToCube.entrySet()
				.stream()
				.filter(c -> isRequested(query.getCube(), allIfEmpty, hasAnyFilter, c))
				.forEach(c -> {
					String cubeName = c.getKey();
					ICubeWrapper cube = c.getValue();

					CubeSchemaMetadata.CubeSchemaMetadataBuilder cubeSchema = CubeSchemaMetadata.builder();

					ColumnarMetadata columns;
					try {
						columns = ColumnarMetadata.from(cacheCubeToColumnToType.getUnchecked(cubeName)).build();
					} catch (RuntimeException e) {
						if (AdhocUnsafe.isFailFast()) {
							throw e;
						} else {
							log.warn("Issue fetching columns from cube={}", cubeName, e);
							columns = ColumnarMetadata.from(Map.of("error", e.getClass())).build();
						}
					}
					cubeSchema.columns(columns);
					cubeSchema.measures(cube.getNameToMeasure()
							.entrySet()
							.stream()
							.collect(ImmutableMap.toImmutableMap(Map.Entry::getKey,
									e -> enrichMeasure(cube.getName(), e.getValue()))));

					Map<String, CustomMarkerMetadata> customMarkerNameToMetadata = new TreeMap<>();

					nameToCustomMarker.stream()
							.filter(customMarker -> customMarker.getKey().getCubeMatcher().match(cubeName))
							.forEach(customMarker -> {
								String customMarkerName = customMarker.getKey().getName();
								CustomMarkerMetadata customMarkerMetadata =
										customMarker.getValue().snapshot(customMarkerName);
								CustomMarkerMetadata previous =
										customMarkerNameToMetadata.put(customMarkerName, customMarkerMetadata);

								if (previous != null) {
									log.warn("cube={} customMarker={} matches multiple metadata: {} and {}",
											cubeName,
											customMarkerName,
											customMarkerMetadata,
											previous);
								}
							});
					cubeSchema.customMarkers(customMarkerNameToMetadata);

					metadata.cube(cubeName, cubeSchema.build());
				});

		nameToForest.entrySet()
				.stream()
				.filter(e -> isRequested(query.getForest(), allIfEmpty, hasAnyFilter, e))
				.forEach(e -> {
					String name = e.getKey();
					IMeasureForest forest = e.getValue();
					List<IMeasure> measures = ImmutableList.copyOf(forest.getNameToMeasure().values());
					metadata.forest(name, measures);
				});

		nameToTable.entrySet()
				.stream()
				.filter(e -> isRequested(query.getTable(), allIfEmpty, hasAnyFilter, e))
				.forEach(e -> {
					String name = e.getKey();
					ITableWrapper table = e.getValue();
					// TODO Should we cache?
					metadata.table(name, ColumnarMetadata.from(table.getColumnTypes()).build());
				});

		// nameToQuery.forEach((name, query) -> {
		// metadata.query(name, AdhocQuery.edit(query).build());
		// });

		return metadata.build();
	}

	protected boolean isRequested(Optional<String> optRequested,
			boolean allIfEmpty,
			boolean hasAnyFilter,
			Map.Entry<String, ? extends IHasName> e) {
		return allIfEmpty && !hasAnyFilter && optRequested.isEmpty()
				|| optRequested.isPresent() && optRequested.get().equals(e.getKey());
	}

	@Override
	public ITabularView execute(String cube, ICubeQuery query) {
		ICubeWrapper cubeWrapper = nameToCube.get(cube);

		if (cubeWrapper == null) {
			throw new IllegalArgumentException("No cube named %s".formatted(cube));
		}

		ICubeQuery transcodedQuery = transcodeQuery(cubeWrapper, query);

		if (query.isDebugOrExplain()) {
			// This can be helpful to debug why some `c=v` filters are turned into `c.toString() matches v`
			log.info("[EXPLAIN] Transcoded to {} from {}", transcodedQuery, query);
		}

		return cubeWrapper.execute(transcodedQuery);
	}

	/**
	 * 
	 * @param cubeWrapper
	 * @param query
	 *            a query typically received by API, hence transcoded by Jackson. it would typically have improper types
	 *            on some filters.
	 * @return
	 */
	protected ICubeQuery transcodeQuery(ICubeWrapper cubeWrapper, ICubeQuery query) {
		return CubeQuery.edit(query).filter(transcodeFilter(cubeWrapper, query.getFilter())).build();
	}

	protected ISliceFilter transcodeFilter(ICubeWrapper cubeWrapper, ISliceFilter filter) {
		ICustomTypeManagerSimple customTypeManager = makeTypeManager(cubeWrapper);
		return MoreFilterHelpers.transcodeFilter(customTypeManager, ITableTranscoder.identity(), filter);
	}

	protected ICustomTypeManagerSimple makeTypeManager(ICubeWrapper cubeWrapper) {
		Collection<? extends ColumnMetadata> cubeColumns = cacheCubeToColumnToType.getUnchecked(cubeWrapper.getName());

		Map<String, Class<?>> columnToType = new HashMap<>();

		cubeColumns.forEach(column -> columnToType.put(column.getName(), column.getType()));

		ICustomTypeManagerSimple customTypeManager =
				CubeWrapperTypeTranscoder.builder().columnToTypes(columnToType).build();
		return customTypeManager;
	}

	public void registerCube(ICubeWrapper cube) {
		nameToCube.put(cube.getName(), cube);
	}

	public void registerTable(ITableWrapper table) {
		nameToTable.put(table.getName(), table);
	}

	public void registerForest(IMeasureForest forest) {
		nameToForest.put(forest.getName(), forest);
	}

	/**
	 * 
	 * @param name
	 *            some identifier for the customMarker
	 * @param cubeMatcher
	 * @param customMarker
	 */
	public void registerCustomMarker(String name,
			IValueMatcher cubeMatcher,
			CustomMarkerMetadataGenerator customMarker) {
		CustomMarkerMatchingKey matchingKey =
				CustomMarkerMatchingKey.builder().name(name).cubeMatcher(cubeMatcher).build();
		nameToCustomMarker.add(Map.entry(matchingKey, customMarker));
	}

	// public void registerMeasure(String measureBagName, IMeasure measure) {
	// AdhocMeasureBag measureBag = (AdhocMeasureBag) nameToMeasure.computeIfAbsent(measureBagName,
	// k -> AdhocMeasureBag.builder().name(measureBagName).build());
	// measureBag.addMeasure(measure);
	// }

	/**
	 * @param columnId
	 * @param valueMatcher
	 * @param limit
	 *            if `-1` no limit, else the maximum number of coordinates to return.
	 * @return
	 */
	public CoordinatesSample getCoordinates(ColumnIdentifier columnId, IValueMatcher valueMatcher, int limit) {
		if (columnId.isCubeElseTable()) {
			return nameToCube.get(columnId.getHolder()).getCoordinates(columnId.getColumn(), valueMatcher, limit);
		} else {
			return nameToTable.get(columnId.getHolder()).getCoordinates(columnId.getColumn(), valueMatcher, limit);

		}
	}

	public Map<String, ColumnMetadata> getCubeColumns(String cube) {
		return nameToCube.get(cube).getColumnsAsMap();
	}

	public Collection<ICubeWrapper> getCubes() {
		return nameToCube.values();
	}

	public CubeWrapperBuilder openCubeWrapperBuilder() {
		return CubeWrapper.builder().engine(engine);
	}

	public void tagColumn(ColumnIdentifier columnIdentifier, Set<String> tags) {
		columnToTags.computeIfAbsent(columnIdentifier, k -> new ConcurrentSkipListSet<>()).addAll(tags);
	}

	public void tagMeasure(MeasureIdentifier measureIdentifier, Set<String> tags) {
		measureToTags.computeIfAbsent(measureIdentifier, k -> new ConcurrentSkipListSet<>()).addAll(tags);
	}

	// public void registerQuery(String name, IAdhocQuery query) {
	// nameToQuery.put(name, query);
	// }
}
