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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.cube.IAdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import lombok.Builder;
import lombok.Value;

/**
 * Wraps together a Set of {@link IAdhocTableWrapper}, {@link IMeasureForest}, {@link IAdhocCubeWrapper} and
 * {@link IAdhocQuery}. It is typically used for use through an API.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class AdhocSchema implements IAdhocSchema {
	@Builder.Default
	final IAdhocQueryEngine engine = AdhocQueryEngine.builder().build();

	final Map<String, IAdhocTableWrapper> nameToTable = new ConcurrentHashMap<>();

	final Map<String, IMeasureForest> nameToForest = new ConcurrentHashMap<>();

	final Map<String, IAdhocCubeWrapper> nameToCube = new ConcurrentHashMap<>();

	// final Map<String, IAdhocQuery> nameToQuery = new ConcurrentHashMap<>();

	public void registerCube(String cubeName, String tableName, String forestName) {
		AdhocCubeWrapper cube = AdhocCubeWrapper.builder()
				.name(cubeName)
				.engine(engine)
				.forest(nameToForest.get(forestName))
				.table(nameToTable.get(tableName))
				.build();

		nameToCube.put(cubeName, cube);
	}

	@Override
	public EndpointSchemaMetadata getMetadata() {
		EndpointSchemaMetadata.EndpointSchemaMetadataBuilder metadata = EndpointSchemaMetadata.builder();

		nameToCube.forEach((name, cube) -> {
			CubeSchemaMetadata.CubeSchemaMetadataBuilder cubeSchema = CubeSchemaMetadata.builder();

			cubeSchema.columns(ColumnarMetadata.from(cube.getColumns()));
			cubeSchema.measures(cube.getNameToMeasure());

			metadata.cube(name, cubeSchema.build());
		});

		nameToForest.forEach((name, measureBag) -> {
			List<IMeasure> measures = ImmutableList.copyOf(measureBag.getNameToMeasure().values());
			metadata.measureBag(name, measures);
		});

		nameToTable.forEach((name, table) -> {
			metadata.table(name, ColumnarMetadata.from(table.getColumns()));
		});

		// nameToQuery.forEach((name, query) -> {
		// metadata.query(name, AdhocQuery.edit(query).build());
		// });

		return metadata.build();
	}

	@Override
	public ITabularView execute(String cube, IAdhocQuery query, Set<? extends IQueryOption> options) {
		IAdhocCubeWrapper cubeWrapper = nameToCube.get(cube);

		if (cubeWrapper == null) {
			throw new IllegalArgumentException("No cube named %s".formatted(cube));
		}

		return cubeWrapper.execute(query, options);
	}

	public void registerTable(IAdhocTableWrapper table) {
		nameToTable.put(table.getName(), table);
	}

	public void registerMeasureBag(IMeasureForest measureBag) {
		nameToForest.put(measureBag.getName(), measureBag);
	}

	// public void registerMeasure(String measureBagName, IMeasure measure) {
	// AdhocMeasureBag measureBag = (AdhocMeasureBag) nameToMeasure.computeIfAbsent(measureBagName,
	// k -> AdhocMeasureBag.builder().name(measureBagName).build());
	// measureBag.addMeasure(measure);
	// }

	/**
	 * 
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

	// public void registerQuery(String name, IAdhocQuery query) {
	// nameToQuery.put(name, query);
	// }
}
