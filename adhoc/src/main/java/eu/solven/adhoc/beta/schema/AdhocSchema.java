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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.IHasName;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps together a Set of {@link ITableWrapper}, {@link IMeasureForest}, {@link ICubeWrapper} and {@link IAdhocQuery}.
 * It is typically used for use through an API/Catalog.
 *
 * @author Benoit Lacelle
 */
// @Value
@Builder
@Slf4j
public class AdhocSchema implements IAdhocSchema {
	@Builder.Default
	@NonNull
	final IAdhocQueryEngine engine = AdhocQueryEngine.builder().build();

	final Map<String, ITableWrapper> nameToTable = new ConcurrentHashMap<>();

	final Map<String, IMeasureForest> nameToForest = new ConcurrentHashMap<>();

	final Map<String, ICubeWrapper> nameToCube = new ConcurrentHashMap<>();

	final List<Map.Entry<CustomMarkerMatchingKey, CustomMarkerMetadataGenerator>> nameToCustomMarker =
			new CopyOnWriteArrayList<>();

	// final Map<String, IAdhocQuery> nameToQuery = new ConcurrentHashMap<>();

	@Value
	@Builder
	public static class CustomMarkerMatchingKey {
		String name;
		IValueMatcher cubeMatcher;
	}

	public CubeWrapper registerCube(String cubeName, String tableName, String forestName) {
		CubeWrapper cube = CubeWrapper.builder()
				.name(cubeName)
				.engine(engine)
				.forest(nameToForest.get(forestName))
				.table(nameToTable.get(tableName))
				.build();

		nameToCube.put(cubeName, cube);

		return cube;
	}

	@Override
	public EndpointSchemaMetadata getMetadata(AdhocSchemaQuery query, boolean allIfEmpty) {
		EndpointSchemaMetadata.EndpointSchemaMetadataBuilder metadata = EndpointSchemaMetadata.builder();

		nameToCube.entrySet().stream().filter(e -> isRequested(query.getCube(), allIfEmpty, e)).forEach(e -> {
			String cubeName = e.getKey();
			ICubeWrapper cube = e.getValue();

			CubeSchemaMetadata.CubeSchemaMetadataBuilder cubeSchema = CubeSchemaMetadata.builder();

			// `getColumns` is an expensive operations, as it analyzes the underlying table
			// TODO Should we cache?
			cubeSchema.columns(ColumnarMetadata.from(cube.getColumns()));
			cubeSchema.measures(cube.getNameToMeasure());

			Map<String, CustomMarkerMetadata> customMarkerNameToMetadata = new TreeMap<>();

			nameToCustomMarker.stream()
					.filter(customMarker -> customMarker.getKey().getCubeMatcher().match(cubeName))
					.forEach(customMarker -> {
						String customMarkerName = customMarker.getKey().getName();
						CustomMarkerMetadata customMarkerMetadata = customMarker.getValue().snapshot(customMarkerName);
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

		nameToForest.entrySet().stream().filter(e -> isRequested(query.getForest(), allIfEmpty, e)).forEach(e -> {
			String name = e.getKey();
			IMeasureForest forest = e.getValue();
			List<IMeasure> measures = ImmutableList.copyOf(forest.getNameToMeasure().values());
			metadata.measureBag(name, measures);
		});

		nameToTable.entrySet().stream().filter(e -> isRequested(query.getTable(), allIfEmpty, e)).forEach(e -> {
			String name = e.getKey();
			ITableWrapper table = e.getValue();
			// TODO Should we cache?
			metadata.table(name, ColumnarMetadata.from(table.getColumns()));
		});

		// nameToQuery.forEach((name, query) -> {
		// metadata.query(name, AdhocQuery.edit(query).build());
		// });

		return metadata.build();
	}

	protected boolean isRequested(Optional<String> optRequested,
			boolean allIfEmpty,
			Map.Entry<String, ? extends IHasName> e) {
		return allIfEmpty && optRequested.isEmpty()
				|| optRequested.isPresent() && optRequested.get().equals(e.getKey());
	}

	@Override
	public ITabularView execute(String cube, IAdhocQuery query) {
		ICubeWrapper cubeWrapper = nameToCube.get(cube);

		if (cubeWrapper == null) {
			throw new IllegalArgumentException("No cube named %s".formatted(cube));
		}

		return cubeWrapper.execute(query);
	}

	public void registerCube(ICubeWrapper cube) {
		nameToCube.put(cube.getName(), cube);
	}

	public void registerTable(ITableWrapper table) {
		nameToTable.put(table.getName(), table);
	}

	public void registerForest(IMeasureForest measureBag) {
		nameToForest.put(measureBag.getName(), measureBag);
	}

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

	public Map<String, Class<?>> getCubeColumns(String cube) {
		return nameToCube.get(cube).getColumns();
	}

	public Collection<ICubeWrapper> getCubes() {
		return nameToCube.values();
	}

	public CubeWrapperBuilder openCubeWrapperBuilder() {
		return CubeWrapper.builder().engine(engine);
	}

	// public void registerQuery(String name, IAdhocQuery query) {
	// nameToQuery.put(name, query);
	// }
}
