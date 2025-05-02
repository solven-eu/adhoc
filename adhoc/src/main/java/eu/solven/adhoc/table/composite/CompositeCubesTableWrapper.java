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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.MultimapBuilder.SetMultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.ColumnMetadata;
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
import eu.solven.adhoc.measure.IHasMeasures;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.IHasAggregationKey;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.composite.CompositeCubeHelper.CompatibleMeasures;
import eu.solven.adhoc.table.composite.SubMeasureAsAggregator.SubMeasureAsAggregatorBuilder;
import eu.solven.adhoc.util.NotYetImplementedException;
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

	// A column which shall not exist in any underlying cube
	// Useful to let a user slice through underlying cubes
	@NonNull
	@Default
	// Default name refer to `adhoc` to insist on the fact it does not come from the data
	final Optional<String> optCubeSlicer = Optional.of("cubeSlicer");

	@Override
	public List<ColumnMetadata> getColumns() {
		SetMultimap<String, ColumnMetadata> columnToJavaType = SetMultimapBuilder.treeKeys().hashSetValues().build();

		cubes.stream().flatMap(cube -> cube.getColumns().stream()).forEach(c -> columnToJavaType.put(c.getName(), c));

		optCubeSlicer.ifPresent(cubeColumn -> columnToJavaType.put(cubeColumn,
				ColumnMetadata.builder().name(cubeColumn).tag("adhoc").build()));

		return columnToJavaType.asMap().entrySet().stream().map(e -> {
			return ColumnMetadata.merge(e.getValue());
		}).toList();
	}

	@Override
	public String toString() {
		return "CompositeCube over " + cubes.stream().map(ICubeWrapper::getName).collect(Collectors.joining("&"));
	}

	@Value
	@Builder
	public static class SubQueryParameters {
		@NonNull
		ICubeWrapper cube;
		@NonNull
		ICubeQuery query;
	}

	@Override
	public ITabularRecordStream streamSlices(ExecutingQueryContext executingQueryContext, TableQueryV2 compositeQuery) {
		if (executingQueryContext.getTable() != this) {
			throw new IllegalStateException(
					"Inconsistent tables: %s vs %s".formatted(executingQueryContext.getTable(), this));
		}

		IAdhocGroupBy compositeGroupBy = compositeQuery.getGroupBy();

		Map<String, ICubeQuery> cubeToQuery = new LinkedHashMap<>();

		cubes.stream().filter(subCube -> isEligible(subCube, compositeQuery)).forEach(subCube -> {
			ICubeQuery subQuery = makeSubQuery(executingQueryContext, compositeQuery, compositeGroupBy, subCube);

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
			Set<String> subColumns = subCube.getColumnTypes().keySet();

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

	protected CompatibleMeasures computeSubMeasures(TableQueryV2 compositeQuery,
			IHasMeasures subCube,
			Set<String> subColumns) {
		if (compositeQuery.getAggregators().stream().anyMatch(fa -> !IAdhocFilter.MATCH_ALL.equals(fa.getFilter()))) {
			// TODO Could this be managed with a Filtrator? Leaving the `FILTER` management to the subCube?
			throw new NotYetImplementedException(
					"FILTER in CompositeCube is not supported yet: %s".formatted(compositeQuery));
		}

		Set<String> cubeMeasures = subCube.getNameToMeasure().keySet();

		// Measures which are known by the subCube
		Set<IMeasure> predefinedMeasures = compositeQuery.getAggregators()
				.stream()
				// The subCube measure is in the Aggregator columnName
				// the Aggregator name may be an alias in the compositeCube (e.g. in case of conflict)
				.filter(a -> cubeMeasures.contains(a.getAggregator().getColumnName()))
				.map(fa -> IMeasure.alias(fa.getAlias(), fa.getAggregator().getColumnName()))
				.collect(Collectors.toCollection(LinkedHashSet::new));

		// We also propagate to the subCube some measures which definition can be computed on the fly
		Set<FilteredAggregator> defined = compositeQuery.getAggregators()
				.stream()
				// subCube does not know about `measure=k1`
				.filter(a -> !cubeMeasures.contains(a.getAggregator().getColumnName()))
				// But the subCube has a `column=k1` and we want to aggregate over `k1`
				// So we propagate the provided definition to the subCube
				// TODO Could we also add some transformators?
				.filter(a -> isColumnAvailable(subColumns, a.getAggregator().getColumnName()))
				.collect(Collectors.toCollection(LinkedHashSet::new));

		CompatibleMeasures compatible = CompatibleMeasures.builder()
				.predefined(predefinedMeasures)
				.defined(defined.stream()
						.map(fa -> FilteredAggregator.toAggregator(fa))
						.collect(Collectors.toCollection(LinkedHashSet::new)))
				.build();
		return compatible;
	}

	protected boolean isEligible(ICubeWrapper subCube, TableQueryV2 compositeQuery) {
		if (EmptyAggregation.isEmpty(compositeQuery.getAggregators())) {
			// Requesting for slices: to be propagated to each underlying cube
			return true;
		} else {
			Set<String> subColumns = subCube.getColumnTypes().keySet();
			CompatibleMeasures compatible = computeSubMeasures(compositeQuery, subCube, subColumns);

			// The cube is eligible if it has at least one relevant measure amongst the queried ones
			return !compatible.isEmpty();
		}
	}

	protected ICubeQuery makeSubQuery(ExecutingQueryContext executingQueryContext,
			TableQueryV2 compositeQuery,
			IAdhocGroupBy compositeGroupBy,
			ICubeWrapper subCube) {
		Set<String> subColumns = subCube.getColumnTypes().keySet();

		// groupBy only by relevant columns. Other columns are ignored
		NavigableMap<String, IAdhocColumn> subGroupBy = new TreeMap<>(compositeGroupBy.getNameToColumn());
		subGroupBy.keySet().retainAll(subColumns);

		IAdhocFilter compositeFilter = compositeQuery.getFilter();
		IAdhocFilter subFilter = filterForColumns(compositeFilter, subColumns);

		CompatibleMeasures subMeasures = computeSubMeasures(compositeQuery, subCube, subColumns);

		ICubeQuery query = CubeQuery.edit(compositeQuery)
				.filter(subFilter)
				.groupBy(GroupByColumns.of(subGroupBy.values()))
				// Reference the measures already known by the subCube
				.measures(subMeasures.getPredefined())
				// Add some measure given their definitions
				.measures(subMeasures.getDefined())

				// Some of the queried measures may be unknown to some cubes
				// (Is this relevant given we queried only the relevant measures?)
				.option(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY)
				// We want carriers from the underlying cubes, to aggregate them properly
				// (e.g. a Top2 cross-cubes needs each cube to return its Top2)
				.option(StandardQueryOptions.AGGREGATION_CARRIERS_STAY_WRAPPED)

				.build();

		return AdhocSubQuery.builder().subQuery(query).parentQueryId(executingQueryContext.getQueryId()).build();
	}

	protected boolean isColumnAvailable(Set<String> subColumns, String subColumn) {
		// `*` is relevant if we're requesting `COUNT(*)`
		return ICountMeasuresConstants.ASTERISK.equals(subColumn) || subColumns.contains(subColumn);
	}

	// Manages concurrency: the logic here should be strictly minimal on-top of concurrency
	protected Map<String, ITabularView> executeSubQueries(ExecutingQueryContext executingQueryContext,
			Map<String, ICubeQuery> cubeToQuery) {
		Map<String, ICubeWrapper> nameToCube = getNameToCube();

		try {
			// https://stackoverflow.com/questions/21163108/custom-thread-pool-in-java-8-parallel-stream
			return executingQueryContext.getFjp().submit(() -> {
				Stream<Entry<String, ICubeQuery>> stream = cubeToQuery.entrySet().stream();

				if (executingQueryContext.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
					stream = stream.parallel();
				}

				return stream.collect(Collectors.toMap(Entry::getKey, e -> {
					ICubeWrapper subCube = nameToCube.get(e.getKey());
					ICubeQuery query = e.getValue();
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

	protected ITabularView executeSubQuery(ICubeWrapper subCube, ICubeQuery query) {
		return subCube.execute(query);
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
	 *            a {@link ICubeQuery} filter
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
	 * This will register all measure of underlying cubes into this {@link IMeasureForest}. It will enable querying
	 * subCube measures from the composite cube.
	 * 
	 * @param compositeForest
	 *            the forest with composite cube own measures.
	 */
	public IMeasureForest injectUnderlyingMeasures(IMeasureForest compositeForest) {
		Set<String> compositeMeasures = new HashSet<>(compositeForest.getNameToMeasure().keySet());

		SetMultimap<String, String> measureToCubes = HashMultimap.create();

		Set<IMeasure> measuresToAdd = new HashSet<>();

		cubes.forEach(subCube -> {
			subCube.getMeasures().stream().forEach(subMeasure -> {
				String subMeasureName = subMeasure.getName();

				String compositeMeasureName;

				String cubeName = subCube.getName();
				if (compositeMeasures.contains(subMeasureName)) {
					log.debug("{} is a measureName both in composite and in {}", subMeasureName, cubeName);
					compositeMeasureName = conflictingSubMeasureName(subMeasureName, cubeName);

					if (compositeMeasures.contains(compositeMeasureName)) {
						// TODO Loop until we find an available measureName
						throw new IllegalArgumentException(
								"%s is a measure in both composite and %s, and %s is a composite measure"
										.formatted(subMeasureName, cubeName, compositeMeasureName));
					}
				} else {
					compositeMeasureName = subMeasureName;
				}

				measureToCubes.put(compositeMeasureName, cubeName);

				IMeasure compositeMeasure = wrapAsCompositeMeasure(subCube, subMeasure, compositeMeasureName);
				measuresToAdd.add(compositeMeasure);
			});
		});

		measureToCubes.asMap()
				.entrySet()
				.stream()
				.filter(e -> e.getValue().size() >= 2)
				.forEach(e -> log.debug("measure={} is provided by cubes: {}", e.getKey(), e.getValue()));

		MeasureForest.MeasureForestBuilder builder = MeasureForest.edit(compositeForest);

		measuresToAdd.forEach(builder::measure);

		return builder.build();
	}

	/**
	 * 
	 * @param subCube
	 * @param subMeasure
	 *            the measure as defined in the cub cube
	 * @param compositeMeasureName
	 *            the name for given measure in the composite cube
	 * @return
	 */
	protected IMeasure wrapAsCompositeMeasure(ICubeWrapper subCube, IMeasure subMeasure, String compositeMeasureName) {
		SubMeasureAsAggregatorBuilder measureBuilder = SubMeasureAsAggregator.builder()
				// this may or may not match the underlyingCube name
				.name(compositeMeasureName)
				.subMeasure(subMeasure.getName());

		// If some measure is returned by different cubes, we SUM the returned values
		// TODO The aggregationKey should be evaluated given the Set of providing Cubes
		measureBuilder.aggregationKey(aggregationKeyForSubMeasure(subCube, subMeasure))
				.aggregationOptions(aggregationOptionsForSubMeasure(subCube, subMeasure));

		if (subMeasure instanceof IHasUnderlyingMeasures hasUndelryingMeasures) {
			// The underlying measure of subCube measure are useful for information purposes
			// e.g. enabling easy underlying measure selection in Pivotable
			measureBuilder.underlyings(hasUndelryingMeasures.getUnderlyingNames());
		}

		return measureBuilder.build();
	}

	protected String conflictingSubMeasureName(String measureName, String cubeName) {
		return measureName + "." + cubeName;
	}

	protected String aggregationKeyForSubMeasure(ICubeWrapper cube, IMeasure subMeasure) {
		if (subMeasure instanceof IHasAggregationKey hasAggregationKey) {
			return hasAggregationKey.getAggregationKey();
		} else {
			return SumAggregation.KEY;
		}
	}

	protected Map<String, ?> aggregationOptionsForSubMeasure(ICubeWrapper subCube, IMeasure subMeasure) {
		if (subMeasure instanceof IHasAggregationKey hasAggregationKey) {
			return hasAggregationKey.getAggregationOptions();
		} else {
			return Map.of();
		}
	}

}
