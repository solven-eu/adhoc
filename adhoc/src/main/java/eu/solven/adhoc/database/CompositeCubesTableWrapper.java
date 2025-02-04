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
package eu.solven.adhoc.database;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.adhoc.dag.IAdhocCubeWrapper;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.view.ITabularView;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;

/**
 * This enables combining multiple {@link IAdhocTableWrapper}, and to evaluate {@link IMeasure} on top of them.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class CompositeCubesTableWrapper implements IAdhocTableWrapper {

	@NonNull
	@Default
	final String name = "composite";

	@Singular
	final List<IAdhocCubeWrapper> cubes;

	@Override
	public Map<String, Class<?>> getColumns() {
		Map<String, Class<?>> columnToJavaType = new LinkedHashMap<>();

		// TODO Manage conflicts (e.g. same column but different types)
		cubes.forEach(table -> columnToJavaType.putAll(table.getColumns()));

		return columnToJavaType;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public IRowsStream openDbStream(TableQuery dbQuery) {
		Stream<Map<String, ?>> streams = cubes.stream().flatMap(table -> {
			Set<String> measures = dbQuery.getAggregators().stream().map(a -> a.getName()).collect(Collectors.toSet());
			IAdhocQuery query = AdhocQuery.builder()
					.filter(dbQuery.getFilter())
					.groupBy(dbQuery.getGroupBy())
					.measures(measures)
					.build();

			ITabularView view = table.execute(query, Set.of(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY));

			return view.stream((slice, o) -> {
				Map<String, Object> columnsAndMeasures = new LinkedHashMap<>();

				columnsAndMeasures.putAll(slice.getCoordinates());
				columnsAndMeasures.putAll((Map<? extends String, ? extends Object>) o);

				return columnsAndMeasures;
			});
		});

		return new SuppliedRowsStream(dbQuery, () -> streams);
	}

}
