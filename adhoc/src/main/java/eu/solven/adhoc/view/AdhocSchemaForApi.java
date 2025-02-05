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
package eu.solven.adhoc.view;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import eu.solven.adhoc.dag.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.IAdhocCubeWrapper;
import eu.solven.adhoc.dag.IAdhocMeasureBag;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.dag.IHasColumns;
import eu.solven.adhoc.database.IAdhocTableWrapper;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.transformers.IMeasure;
import lombok.Builder;
import lombok.Value;

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

	public Map<String, ?> getMetadata() {
		Map<String, Object> metadata = new LinkedHashMap<>();

		metadata.put("cubes",
				nameToCube.values()
						.stream()
						.collect(Collectors.toMap(IAdhocCubeWrapper::getName, IHasColumns::getColumns)));

		metadata.put("measures",
				nameToMeasure.entrySet()
						.stream()
						.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getNameToMeasure())));

		metadata.put("tables",
				nameToTable.entrySet()
						.stream()
						.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getColumns())));

		metadata.put("queries",
				nameToQuery.entrySet()
						.stream()
						.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));

		return metadata;
	}

	public ITabularView execute(String cube, IAdhocQuery query, Set<? extends IQueryOption> options) {
		return nameToCube.get(cube).execute(query, options);
	}

	public void registerMeasureBag(IAdhocMeasureBag measureBag) {
		nameToMeasure.put(measureBag.getName(), measureBag);
	}

	public void registerMeasure(String measureBagName, IMeasure measure) {
		AdhocMeasureBag measureBag = (AdhocMeasureBag) nameToMeasure.computeIfAbsent(measureBagName, k -> new AdhocMeasureBag(measureBagName));
		measureBag.addMeasure(measure);
	}

	public void registerQuery(String name, IAdhocQuery query) {
		nameToQuery.put(name, query);
	}
}
