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
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import eu.solven.adhoc.dag.IAdhocCubeWrapper;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.groupby.IAdhocColumn;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.storage.DefaultMissingColumnManager;
import eu.solven.adhoc.storage.IMissingColumnManager;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.view.ITabularView;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
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
	@Getter
	final String name = "composite";

	@Singular
	final List<IAdhocCubeWrapper> cubes;

	@Default
	IMissingColumnManager missingColumnManager = new DefaultMissingColumnManager();

	@Override
	public Map<String, Class<?>> getColumns() {
		Map<String, Class<?>> columnToJavaType = new LinkedHashMap<>();

		// TODO Manage conflicts (e.g. same column but different types)
		cubes.forEach(table -> columnToJavaType.putAll(table.getColumns()));

		return columnToJavaType;
	}

	@Override
	public IRowsStream openDbStream(TableQuery compositeQuery) {
		IAdhocGroupBy compositeGroupBy = compositeQuery.getGroupBy();
		IAdhocFilter compositeFilter = compositeQuery.getFilter();

		Stream<Map<String, ?>> streams = cubes.stream().flatMap(cube -> {
			Set<String> measures =
					compositeQuery.getAggregators().stream().map(a -> a.getName()).collect(Collectors.toSet());

			Set<String> cubeColumns = cube.getColumns().keySet();

			// groupBy only by relevant columns. Other columns are ignored
			NavigableMap<String, IAdhocColumn> validGroupBy = new TreeMap<>(compositeGroupBy.getNameToColumn());
			validGroupBy.keySet().retainAll(cubeColumns);

			NavigableSet<String> missingColumns =
					new TreeSet<>(Sets.difference(compositeGroupBy.getNameToColumn().keySet(), cubeColumns));

			IAdhocFilter validFilter = filterForColumns(compositeFilter, cubeColumns);

			IAdhocQuery query = AdhocQuery.builder()
					.filter(validFilter)
					.groupBy(GroupByColumns.of(validGroupBy.values()))
					.measures(measures)
					.debug(compositeQuery.isDebug())
					.explain(compositeQuery.isExplain())
					.build();

			ITabularView view = cube.execute(query, Set.of(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY));

			return view.stream((slice, o) -> {
				Map<String, Object> columnsAndMeasures = new LinkedHashMap<>();

				columnsAndMeasures.putAll(slice.getCoordinates());
				columnsAndMeasures.putAll((Map<? extends String, ? extends Object>) o);

				missingColumns.forEach(column -> columnsAndMeasures.put(column, missingColumn(cube, column)));

				return columnsAndMeasures;
			});
		});

		return new SuppliedRowsStream(compositeQuery, () -> streams);
	}

	private Object missingColumn(IAdhocCubeWrapper cube, String column) {
		return missingColumnManager.onMissingColumn(cube, column);
	}

	private IAdhocFilter filterForColumns(IAdhocFilter filter, Set<String> columns) {
		if (filter instanceof IColumnFilter columnFilter) {
			if (columns.contains(columnFilter.getColumn())) {
				return columnFilter;
			} else {
				return IAdhocFilter.MATCH_ALL;
			}
		} else if (filter instanceof IAndFilter andFilter) {
			List<IAdhocFilter> operands = andFilter.getOperands();
			List<IAdhocFilter> filteredOperands = operands.stream().map(f -> filterForColumns(f, columns)).toList();
			return AndFilter.and(filteredOperands);
		} else {
			throw new UnsupportedOperationException("Not handled: %s".formatted(filter));
		}
	}

}
