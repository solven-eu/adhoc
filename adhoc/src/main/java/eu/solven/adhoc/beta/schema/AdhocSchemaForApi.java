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

import eu.solven.adhoc.beta.schema.SchemaMetadata.SchemaMetadataBuilder;
import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.cube.IAdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.measure.IAdhocMeasureBag;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import lombok.Builder;
import lombok.Value;

/**
 * Wraps together a Set of {@link IAdhocTableWrapper}, {@link IAdhocMeasureBag}, {@link IAdhocCubeWrapper} and
 * {@link IAdhocQuery}. It is typically used for use through an API.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class AdhocSchemaForApi {
	@Builder.Default
	final IAdhocQueryEngine engine = AdhocQueryEngine.builder().build();

	final Map<String, IAdhocTableWrapper> nameToTable = new ConcurrentHashMap<>();

	final Map<String, IAdhocMeasureBag> nameToMeasure = new ConcurrentHashMap<>();

	final Map<String, IAdhocCubeWrapper> nameToCube = new ConcurrentHashMap<>();

	final Map<String, IAdhocQuery> nameToQuery = new ConcurrentHashMap<>();

	public void registerCube(String cubeName, String tableName, String measuresName) {
		AdhocCubeWrapper cube = AdhocCubeWrapper.builder()
				.name(cubeName)
				.engine(engine)
				.measures(nameToMeasure.get(measuresName))
				.table(nameToTable.get(tableName))
				.build();

		nameToCube.put(cubeName, cube);
	}

	public SchemaMetadata getMetadata() {
		SchemaMetadataBuilder metadata = SchemaMetadata.builder();

		nameToCube.forEach((name, cube) -> {
			metadata.cubeToColumn(name, ColumnarMetadata.from(cube.getColumns()));
		});

		nameToMeasure.forEach((name, measureBag) -> {
			List<IMeasure> measures = ImmutableList.copyOf(measureBag.getNameToMeasure().values());
			metadata.bagToMeasure(name, measures);
		});

		nameToTable.forEach((name, table) -> {
			metadata.tableToColumn(name, ColumnarMetadata.from(table.getColumns()));
		});

		nameToQuery.forEach((name, query) -> {
			metadata.nameToQuery(name, AdhocQuery.edit(query).build());
		});

		return metadata.build();
	}

	public ITabularView execute(String cube, IAdhocQuery query, Set<? extends IQueryOption> options) {
		return nameToCube.get(cube).execute(query, options);
	}

	public void registerTable(IAdhocTableWrapper table) {
		nameToTable.put(table.getName(), table);
	}

	public void registerMeasureBag(IAdhocMeasureBag measureBag) {
		nameToMeasure.put(measureBag.getName(), measureBag);
	}

	public void registerMeasure(String measureBagName, IMeasure measure) {
		AdhocMeasureBag measureBag = (AdhocMeasureBag) nameToMeasure.computeIfAbsent(measureBagName,
				k -> AdhocMeasureBag.builder().name(measureBagName).build());
		measureBag.addMeasure(measure);
	}

	public void registerQuery(String name, IAdhocQuery query) {
		nameToQuery.put(name, query);
	}
}
