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
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.dag.context.ExecutingQueryContext;
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
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
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
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.composite.CompositeCubeHelper.CompatibleMeasures;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This enables combining multiple {@link ICubeWrapper}, each appearing as an underlying {@link ITableWrapper}, and to
 * evaluate {@link IMeasure} on top of them.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class CompositeCubesTableWrapper implements ITableWrapper {

	@NonNull
	@Default
	@Getter
	final String name = "composite";

	@Singular
	final List<ICubeWrapper> cubes;

	// Enable managing missing columns for subCubes
	@NonNull
	@Default
	final IColumnsManager columnsManager = ColumnsManager.builder().build();

	@Override
	public Map<String, Class<?>> getColumns() {
		Map<String, Class<?>> columnToJavaType = new LinkedHashMap<>();

		// TODO Manage conflicts (e.g. same column but different types)
		cubes.forEach(cube -> columnToJavaType.putAll(cube.getColumns()));

		return columnToJavaType;
	}

	@Value
	@Builder
	public static class SubQueryParameters {
		ICubeWrapper cube;
		IAdhocQuery query;
	}

	@Override
	public ITabularRecordStream streamSlices(ExecutingQueryContext executingQueryContext, TableQuery compositeQuery) {
		if (executingQueryContext.getTable() != this) {
			throw new IllegalStateException(
					"Inconsistent tables: %s vs %s".formatted(executingQueryContext.getTable(), this));
		}

		IAdhocGroupBy compositeGroupBy = compositeQuery.getGroupBy();

		Map<String, IAdhocQuery> cubeToQuery = new LinkedHashMap<>();

		cubes.stream().filter(subCube -> isEligible(subCube, compositeQuery)).forEach(subCube -> {
			Set<String> subColumns = subCube.getColumns().keySet();

			IAdhocQuery subQuery =
					makeSubQuery(executingQueryContext, compositeQuery, compositeGroupBy, subCube, subColumns);

			var previous = cubeToQuery.put(subCube.getName(), subQuery);
			if (previous != null) {
				throw new IllegalStateException("Multiple cubes are named: " + subCube.getName());
			}
		});

		// Actual execution is the only concurrent section
		final Map<String, ITabularView> cubeToView = executeSubQueries(executingQueryContext, cubeToQuery);

		Map<String, ICubeWrapper> nameToCube = getNameToCube();
		Stream<ITabularRecord> streams = cubeToView.entrySet().stream().flatMap(e -> {
			ICubeWrapper subCube = nameToCube.get(e.getKey());
			Set<String> subColumns = subCube.getColumns().keySet();

			// Columns which are requested (hence present in the composite Cube/ one of the subCube) but missing
			// from current subCube.
			NavigableSet<String> subMissingColumns =
					new TreeSet<>(Sets.difference(compositeGroupBy.getNameToColumn().keySet(), subColumns));

			ITabularView subView = e.getValue();
			return subView.stream((slice) -> {
				return oAsMap -> {
					return transcodeSliceToComposite(subCube, slice, oAsMap, subMissingColumns);
				};
			});
		});

		return new SuppliedTabularRecordStream(compositeQuery, () -> streams);
	}

	protected Map<String, ICubeWrapper> getNameToCube() {
		Map<String, ICubeWrapper> nameToCube = new LinkedHashMap<>();
		cubes.forEach(cube -> nameToCube.put(cube.getName(), cube));
		return nameToCube;
	}

	// Manages concurrency: the logic here should be strictly minimal on-top of concurrency
	protected Map<String, ITabularView> executeSubQueries(ExecutingQueryContext executingQueryContext,
			Map<String, IAdhocQuery> cubeToQuery) {
		Map<String, ICubeWrapper> nameToCube = getNameToCube();

		try {
			// https://stackoverflow.com/questions/21163108/custom-thread-pool-in-java-8-parallel-stream
			return executingQueryContext.getFjp().submit(() -> {
				Stream<Entry<String, IAdhocQuery>> stream = cubeToQuery.entrySet().stream();

				if (executingQueryContext.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
					stream = stream.parallel();
				}

				return stream.collect(Collectors.toMap(e -> e.getKey(), e -> {
					ICubeWrapper subCube = nameToCube.get(e.getKey());
					IAdhocQuery query = e.getValue();
					return executeSubQuery(subCube, query);
				}));

			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted", e);
		} catch (ExecutionException e) {
			throw new IllegalStateException("Failed", e);
		}
	}

	protected ITabularView executeSubQuery(ICubeWrapper subCube, IAdhocQuery query) {
		return subCube.execute(query);
	}

	protected boolean isEligible(ICubeWrapper subCube, TableQuery compositeQuery) {
		if (EmptyAggregation.isEmpty(compositeQuery.getAggregators())) {
			// Requesting for slices: to be propagated to each underlying cube
			return true;
		} else {
			Set<String> subColumns = subCube.getColumns().keySet();
			CompatibleMeasures compatible = computeSubMeasures(compositeQuery, subCube, subColumns);

			return !compatible.isEmpty();
		}
	}

	protected IAdhocQuery makeSubQuery(ExecutingQueryContext executingQueryContext,
			TableQuery compositeQuery,
			IAdhocGroupBy compositeGroupBy,
			ICubeWrapper cube,
			Set<String> cubeColumns) {
		IAdhocFilter compositeFilter = compositeQuery.getFilter();

		// groupBy only by relevant columns. Other columns are ignored
		NavigableMap<String, IAdhocColumn> underlyingGroupBy = new TreeMap<>(compositeGroupBy.getNameToColumn());
		underlyingGroupBy.keySet().retainAll(cubeColumns);

		IAdhocFilter underlyingFilter = filterForColumns(compositeFilter, cubeColumns);

		CompatibleMeasures compatible = computeSubMeasures(compositeQuery, cube, cubeColumns);

		IAdhocQuery query = AdhocQuery.edit(compositeQuery)
				.filter(underlyingFilter)
				.groupBy(GroupByColumns.of(underlyingGroupBy.values()))
				// Reference the measures already known by the subCube
				.measureNames(compatible.getUnderlyingQueryMeasures())
				// Add some measure given their definitions
				.measures(compatible.getMissingButAddableMeasures())

				// Some of the queried measures may be unknown to some cubes
				// (Is this relevant given we queried only the relevant measures?)
				.option(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY)
				// We want carriers from the underlying cubes, to aggregate them properly
				// (e.g. a Top2 cross-cubes needs each cube to return its Top2)
				.option(StandardQueryOptions.AGGREGATION_CARRIERS_STAY_WRAPPED)

				.build();

		return AdhocSubQuery.builder().subQuery(query).parentQueryId(executingQueryContext.getQueryId()).build();
	}

	protected CompatibleMeasures computeSubMeasures(TableQuery compositeQuery,
			ICubeWrapper cube,
			Set<String> cubeColumns) {
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

		CompatibleMeasures compatible = CompatibleMeasures.builder()
				.underlyingQueryMeasures(underlyingQueryMeasures)
				.missingButAddableMeasures(missingButAddableMeasures)
				.build();
		return compatible;
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
	protected ITabularRecord transcodeSliceToComposite(ICubeWrapper cube,
			IAdhocSlice slice,
			Map<String, ?> measures,
			NavigableSet<String> missingColumns) {
		Map<String, Object> aggregates = new LinkedHashMap<>(measures);

		// TODO ensureCapacity given missingColumns
		Map<String, Object> groupBys;
		if (missingColumns.isEmpty()) {
			groupBys = slice.getAdhocSliceAsMap().getCoordinates();
		} else {
			groupBys = new LinkedHashMap<>(slice.getAdhocSliceAsMap().getCoordinates());
			missingColumns.forEach(column -> groupBys.put(column, missingColumn(cube, column)));
		}

		return TabularRecordOverMaps.builder().aggregates(aggregates).slice(groupBys).build();
	}

	protected Object missingColumn(ICubeWrapper cube, String column) {
		return columnsManager.onMissingColumn(cube, column);
	}

	/**
	 *
	 * @param filter
	 *            a {@link IAdhocQuery} filter
	 * @param columns
	 *            the {@link IAdhocColumn} available in a {@link ICubeWrapper}
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
				.forEach(e -> log.debug("measure={} is provided by cubes: {}", e.getKey(), e.getValue()));

		MeasureForest.MeasureForestBuilder builder = MeasureForest.edit(measureBag);

		measuresToAdd.forEach(builder::measure);

		return builder.build();
	}
}
