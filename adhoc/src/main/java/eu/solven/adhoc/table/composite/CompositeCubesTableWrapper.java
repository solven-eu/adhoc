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
package eu.solven.adhoc.table.composite;

import java.util.HashSet;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.DefaultMissingColumnManager;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.IMissingColumnManager;
import eu.solven.adhoc.cube.IAdhocCubeWrapper;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.SuppliedTabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

/**
 * This enables combining multiple {@link IAdhocTableWrapper}, and to evaluate {@link IMeasure} on top of them.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
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
	public ITabularRecordStream streamSlices(TableQuery compositeQuery) {
		IAdhocGroupBy compositeGroupBy = compositeQuery.getGroupBy();

		Stream<ITabularRecord> streams = cubes.stream().flatMap(cube -> {

			Set<String> cubeColumns = cube.getColumns().keySet();

			IAdhocQuery query = makeSubCubeQuery(compositeQuery, compositeGroupBy, cube, cubeColumns);

			ITabularView view = cube.execute(query,
					Set.of(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY,
							StandardQueryOptions.AGGREGATION_CARRIERS_STAY_WRAPPED));

			// Columns which are requested (hence present in the composite Cube/ one of the subCube) but missing from
			// current subCube.
			NavigableSet<String> missingColumns =
					new TreeSet<>(Sets.difference(compositeGroupBy.getNameToColumn().keySet(), cubeColumns));
			return view.stream((slice) -> {
				return o -> {
					Map<String, ?> oAsMap = (Map<String, ?>) o;

					return transcodeSliceToComposite(cube, slice, oAsMap, missingColumns);
				};
			});
		});

		return new SuppliedTabularRecordStream(compositeQuery, () -> streams);
	}

	protected IAdhocQuery makeSubCubeQuery(TableQuery compositeQuery,
			IAdhocGroupBy compositeGroupBy,
			IAdhocCubeWrapper cube,
			Set<String> cubeColumns) {
		IAdhocFilter compositeFilter = compositeQuery.getFilter();

		// groupBy only by relevant columns. Other columns are ignored
		NavigableMap<String, IAdhocColumn> underlyingGroupBy = new TreeMap<>(compositeGroupBy.getNameToColumn());
		underlyingGroupBy.keySet().retainAll(cubeColumns);

		IAdhocFilter underlyingFilter = filterForColumns(compositeFilter, cubeColumns);

		Set<String> cubeMeasures = cube.getNameToMeasure().keySet();

		Set<String> compositeQueryMeasures =
				compositeQuery.getAggregators().stream().map(Aggregator::getName).collect(Collectors.toSet());

		Set<String> underlyingQueryMeasures = Sets.intersection(compositeQueryMeasures, cubeMeasures);

		// TODO Could we also add some transformators?
		Set<Aggregator> missingButAddableMeasures = compositeQuery.getAggregators()
				.stream()
				.filter(a -> !cubeMeasures.contains(a.getName()))
				.filter(a -> cubeColumns.contains(a.getColumnName()))
				.collect(Collectors.toSet());

		IAdhocQuery query = AdhocQuery.builder()
				.filter(underlyingFilter)
				.groupBy(GroupByColumns.of(underlyingGroupBy.values()))
				// Reference the measures already known by the subCube
				.measureNames(underlyingQueryMeasures)
				// Add some measure given their definitions
				.measures(missingButAddableMeasures)
				.customMarker(compositeQuery.getCustomMarker())
				.debug(compositeQuery.isDebug())
				.explain(compositeQuery.isExplain())
				.build();
		return query;
	}

	/**
	 *
	 * @param cube
	 *            the underlying cube
	 * @param slice
	 *            a slice from the underlying cube
	 * @param measures
	 * @param missingColumns
	 *            the columns in the compositeQuery groupBy, missing in the underlying cube
	 * @return
	 */
	protected ITabularRecord transcodeSliceToComposite(IAdhocCubeWrapper cube,
			IAdhocSlice slice,
			Map<String, ?> measures,
			NavigableSet<String> missingColumns) {
		Map<String, Object> aggregates = new LinkedHashMap<>(measures);

		Map<String, Object> groupBys = new LinkedHashMap<>(slice.getAdhocSliceAsMap().getCoordinates());
		missingColumns.forEach(column -> groupBys.put(column, missingColumn(cube, column)));

		return TabularRecordOverMaps.builder().aggregates(aggregates).groupBys(groupBys).build();
	}

	protected Object missingColumn(IAdhocCubeWrapper cube, String column) {
		return missingColumnManager.onMissingColumn(cube, column);
	}

	/**
	 *
	 * @param filter
	 *            a {@link IAdhocQuery} filter
	 * @param columns
	 *            the {@link IAdhocColumn} available in a {@link IAdhocCubeWrapper}
	 * @return the equivalent {@link IAdhocFilter} given the subset of columns
	 */
	protected IAdhocFilter filterForColumns(IAdhocFilter filter, Set<String> columns) {
		if (filter.isMatchAll() || filter.isMatchNone()) {
			return filter;
		} else if (filter instanceof IColumnFilter columnFilter) {
			if (columns.contains(columnFilter.getColumn())) {
				return columnFilter;
			} else {
				return IAdhocFilter.MATCH_ALL;
			}
		} else if (filter instanceof IAndFilter andFilter) {
			Set<IAdhocFilter> operands = andFilter.getOperands();
			List<IAdhocFilter> filteredOperands = operands.stream().map(f -> filterForColumns(f, columns)).toList();
			return AndFilter.and(filteredOperands);
		} else if (filter instanceof IOrFilter orFilter) {
			Set<IAdhocFilter> operands = orFilter.getOperands();
			List<IAdhocFilter> filteredOperands = operands.stream()
					.map(f -> filterForColumns(f, columns))
					// In a OR, matchAll should be discarded individually, else the whole OR is matchAll
					// It assumes the initial operands where not matchAll: this is guaranteed by previous call to
					// OrFilter.isMatchAll
					.filter(f -> !f.isMatchAll())
					.toList();
			return OrFilter.or(filteredOperands);
		} else {
			throw new UnsupportedOperationException("Not handled: %s".formatted(filter));
		}
	}

	/**
	 * This will register all measure of underlying cubes into this measure bag. It will enable querying underlying cube
	 * measures from the composite cube.
	 * 
	 * @param measureBag
	 *            the measureBag to be applied on top of this composite cube
	 */
	public IMeasureForest injectUnderlyingMeasures(IMeasureForest measureBag) {
		// The aggregationKey is important in case multiple cubes contribute to the same measure

		Set<String> compositeMeasures = new HashSet<>(measureBag.getNameToMeasure().keySet());

		SetMultimap<String, String> measureToCubes = HashMultimap.create();

		Set<IMeasure> measuresToAdd = new HashSet<>();

		cubes.forEach(cube -> {
			cube.getNameToMeasure().values().stream().forEach(underlyingMeasure -> {
				String measureName = underlyingMeasure.getName();

				String compositeMeasureName;

				String cubeName = cube.getName();
				if (compositeMeasures.contains(measureName)) {
					log.debug("{} is a measureName both in composite and in {}", measureName, cubeName);
					compositeMeasureName = measureName + "." + cubeName;

					if (compositeMeasures.contains(compositeMeasureName)) {
						// TODO Loop until we find an available measureName
						throw new IllegalArgumentException(
								"%s is a measure in both composite and %s, and %s is a composite measure"
										.formatted(measureName, cubeName, compositeMeasureName));
					}
				} else {
					compositeMeasureName = measureName;
				}

				measureToCubes.put(compositeMeasureName, cubeName);

				Aggregator compositeMeasure = Aggregator.builder()
						.name(compositeMeasureName)
						// If some measure is returned by different cubes, we SUM the returned values
						.aggregationKey(SumAggregation.KEY)
						.build();
				measuresToAdd.add(compositeMeasure);
			});
		});

		measureToCubes.asMap()
				.entrySet()
				.stream()
				.filter(e -> e.getValue().size() >= 2)
				.forEach(e -> log.info("measure={} is provided by cubes: {}", e.getKey(), e.getValue()));

		MeasureForest.MeasureForestBuilder builder = MeasureForest.edit(measureBag);

		measuresToAdd.forEach(builder::measure);

		return builder.build();
	}
}
