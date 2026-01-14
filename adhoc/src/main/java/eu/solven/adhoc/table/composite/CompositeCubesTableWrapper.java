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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.MultimapBuilder.SetMultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.column.ColumnMetadata.ColumnMetadataBuilder;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.SuppliedTabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.IHasAggregationKey;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.spring.IHasHealthDetails;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.composite.CompositeCubeHelper.CompatibleMeasures;
import eu.solven.adhoc.table.composite.SubMeasureAsAggregator.SubMeasureAsAggregatorBuilder;
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
public class CompositeCubesTableWrapper implements ITableWrapper, IHasHealthDetails {

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
	// Default name starts with `~` to ensure it is after most standard column names
	final Optional<String> optCubeSlicer = Optional.of("~CompositeSlicer");

	@Override
	public List<ColumnMetadata> getColumns() {
		SetMultimap<String, ColumnMetadata> columnToMeta = SetMultimapBuilder.treeKeys().hashSetValues().build();
		SetMultimap<String, String> columnToCubes = SetMultimapBuilder.treeKeys().hashSetValues().build();

		cubes.stream().forEach(cube -> {
			cube.getColumns().forEach(c -> {
				String columnName = c.getName();

				columnToMeta.put(columnName, c);
				columnToCubes.put(columnName, cube.getName());
			});
		});

		// Add a column enables to groupBy/filter through subCubes
		optCubeSlicer.ifPresent(cubeColumn -> columnToMeta.put(cubeColumn,
				ColumnMetadata.builder().name(cubeColumn).tag("meta").type(String.class).build()));

		return columnToMeta.asMap()
				.entrySet()
				.stream()
				// merge types through subCubes
				.map(e -> {
					return ColumnMetadata.merge(e.getValue());
				})
				// add composite-transverse tag
				.map(c -> {
					ColumnMetadataBuilder builder = c.toBuilder();
					Set<String> cubesWithColumn = columnToCubes.get(c.getName());
					if (optCubeSlicer.isPresent() && optCubeSlicer.get().equals(c.getName())
							|| cubesWithColumn.size() == cubes.size()) {
						return builder.tag("composite-full").build();
					} else {
						// Current column is unknown by some cube
						builder.tag("composite-partial");

						cubes.forEach(cube -> {
							String cubeName = cube.getName();
							if (cubesWithColumn.contains(cubeName)) {
								builder.tag("composite-known:" + cubeName);
							} else {
								builder.tag("composite-unknown:" + cubeName);
							}
						});

						return builder.build();
					}
				})
				.toList();
	}

	@Override
	public String toString() {
		return "CompositeCube over " + cubes.stream().map(ICubeWrapper::getName).collect(Collectors.joining("&"));
	}

	/**
	 * The parameters of a {@link ICubeQuery} for a sub- {@link ICubeWrapper} .
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class SubQueryParameters {
		@NonNull
		ICubeWrapper cube;
		@NonNull
		ICubeQuery query;
	}

	@Override
	public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV2 compositeQuery) {
		if (!Objects.equals(this, queryPod.getTable())) {
			throw new IllegalStateException("Inconsistent tables: %s vs %s".formatted(queryPod.getTable(), this));
		}

		checkColumns(compositeQuery);

		IAdhocGroupBy compositeGroupBy = compositeQuery.getGroupBy();

		Map<String, ICubeQuery> cubeToQuery = new LinkedHashMap<>();

		cubes.stream().filter(subCube -> isEligible(subCube, compositeQuery)).forEach(subCube -> {
			ICubeQuery subQuery = makeSubQuery(queryPod, compositeQuery, compositeGroupBy, subCube);

			var previous = cubeToQuery.put(subCube.getName(), subQuery);
			if (previous != null) {
				throw new IllegalStateException("Multiple cubes are named: " + subCube.getName());
			}
		});

		// Actual execution is the only concurrent section
		final Map<String, ITabularView> cubeToView = executeSubQueries(queryPod, cubeToQuery);

		// not distinct slices as different subCubes may refer to the same slices
		return new SuppliedTabularRecordStream(compositeQuery, false, () -> openStream(compositeGroupBy, cubeToView));
	}

	/**
	 * This method will check the columns in the compositeQuery are valid. This is done early as in later phase, each
	 * subCube will discard unknown columns, supposing another cube will take it in charge.
	 * 
	 * @param compositeQuery
	 */
	protected void checkColumns(TableQueryV2 compositeQuery) {
		NavigableSet<String> groupedByColumns = new TreeSet<>(compositeQuery.getGroupBy().getNameToColumn().keySet());
		Set<String> filteredColumns = new TreeSet<>(FilterHelpers.getFilteredColumns(compositeQuery.getFilter()));

		optCubeSlicer.ifPresent(cubeSlicer -> {
			groupedByColumns.remove(cubeSlicer);
			filteredColumns.remove(cubeSlicer);
		});

		// TODO Should we handle cubes concurrently? It may help given `.getColumnsAsMap` can be slow, but it would make
		// it more difficult to stop once all columns are considered known.
		for (ICubeWrapper cube : cubes) {
			Set<String> cubeColumns = cube.getColumnsAsMap().keySet();
			groupedByColumns.removeAll(cubeColumns);
			filteredColumns.removeAll(cubeColumns);

			if (groupedByColumns.isEmpty() && filteredColumns.isEmpty()) {
				// leave early as the columns looks legitimate, and `.getColumnsAsMap` can be a slow operation
				break;
			}
		}

		if (!groupedByColumns.isEmpty()) {
			throw new IllegalArgumentException(
					"unknown groupedBy columns: %s for query=%s".formatted(groupedByColumns, compositeQuery));
		} else if (!filteredColumns.isEmpty()) {
			throw new IllegalArgumentException(
					"unknown filtered columns: %s for query=%s".formatted(filteredColumns, compositeQuery));
		}

	}

	protected Stream<ITabularRecord> openStream(IAdhocGroupBy compositeGroupBy,
			final Map<String, ITabularView> cubeToView) {
		Map<String, ICubeWrapper> nameToCube = getNameToCube();

		return cubeToView.entrySet().stream().flatMap(e -> {
			ICubeWrapper subCube = nameToCube.get(e.getKey());
			Set<String> subColumns = subCube.getColumnsAsMap().keySet();

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
	}

	protected Map<String, ICubeWrapper> getNameToCube() {
		Map<String, ICubeWrapper> nameToCube = new LinkedHashMap<>();
		cubes.forEach(cube -> nameToCube.put(cube.getName(), cube));
		return nameToCube;
	}

	protected CompatibleMeasures computeSubMeasures(TableQueryV2 compositeQuery,
			ICubeWrapper subCube,
			Predicate<String> isSubColumn) {
		Set<String> cubeMeasures = subCube.getNameToMeasure().keySet();

		// Measures which are known by the subCube
		Set<IMeasure> predefinedMeasures = compositeQuery.getAggregators()
				.stream()
				// The subCube measure is in the Aggregator columnName
				// the Aggregator name may be an alias in the compositeCube (e.g. in case of conflict)
				.filter(a -> cubeMeasures.contains(a.getAggregator().getColumnName()))
				.map(fa -> {
					ISliceFilter compositeFilter = fa.getFilter();
					if (compositeFilter.isMatchAll()) {
						return IMeasure.alias(fa.getAlias(), fa.getAggregator().getColumnName());
					} else {
						ISliceFilter subFilter = filterForColumns(subCube, compositeFilter, isSubColumn);

						return Filtrator.builder()
								.name(fa.getAlias())
								.underlying(fa.getAggregator().getColumnName())
								.filter(subFilter)
								.build();
					}
				})
				.collect(Collectors.toCollection(LinkedHashSet::new));

		// We also propagate to the subCube some measures which definition can be computed on the fly
		Set<FilteredAggregator> defined = compositeQuery.getAggregators()
				.stream()
				// subCube does not know about `measure=k1`
				.filter(a -> !cubeMeasures.contains(a.getAggregator().getColumnName()))
				// But the subCube has a `column=k1` and we want to aggregate over `k1`
				// So we propagate the provided definition to the subCube
				// TODO Could we also add some transformators?
				.filter(a -> isColumnAvailable(isSubColumn, a.getAggregator().getColumnName()))
				.collect(Collectors.toCollection(LinkedHashSet::new));

		return CompatibleMeasures.builder()
				.predefined(predefinedMeasures)
				.defined(defined.stream()
						.map(FilteredAggregator::toAggregator)
						.collect(Collectors.toCollection(LinkedHashSet::new)))
				.build();
	}

	protected boolean isEligible(ICubeWrapper subCube, TableQueryV2 compositeQuery) {
		if (EmptyAggregation.isEmpty(compositeQuery.getAggregators())) {
			// Requesting for slices: to be propagated to each underlying cube
			return true;
		} else {
			Predicate<String> isSubColumn = makeSubColumnPredicate(subCube);
			CompatibleMeasures compatible = computeSubMeasures(compositeQuery, subCube, isSubColumn);

			// The cube is eligible if it has at least one relevant measure amongst the queried ones
			return !compatible.isEmpty();
		}
	}

	protected ICubeQuery makeSubQuery(QueryPod queryPod,
			TableQueryV2 compositeQuery,
			IAdhocGroupBy compositeGroupBy,
			ICubeWrapper subCube) {
		Predicate<String> subCubeKnownMeasure = makeSubColumnPredicate(subCube);

		// groupBy only by relevant columns. Other columns are ignored
		NavigableMap<String, IAdhocColumn> subGroupBy = new TreeMap<>(compositeGroupBy.getNameToColumn());
		subGroupBy.keySet().removeIf(c -> !subCubeKnownMeasure.test(c));

		ISliceFilter compositeFilter = compositeQuery.getFilter();
		ISliceFilter subFilter = filterForColumns(subCube, compositeFilter, subCubeKnownMeasure);

		CompatibleMeasures subMeasures = computeSubMeasures(compositeQuery, subCube, subCubeKnownMeasure);

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

		return AdhocSubQuery.builder().subQuery(query).parentQueryId(queryPod.getQueryId()).build();
	}

	protected Predicate<String> makeSubColumnPredicate(ICubeWrapper subCube) {
		Set<String> subColumns = subCube.getColumnsAsMap().keySet();

		// TODO Enable a subColumn to allow additional columns given some additional Predicate
		// It is useful to handle the fact a ICubeWrapper does not express explicitely all its columns (e.g. due to
		// aliasing)
		return subColumns::contains;
	}

	protected boolean isColumnAvailable(Predicate<String> isSubColumn, String subColumn) {
		// `*` is relevant if we're requesting `COUNT(*)`
		return ICountMeasuresConstants.ASTERISK.equals(subColumn) || isSubColumn.test(subColumn);
	}

	// Manages concurrency: the logic here should be strictly minimal on-top of concurrency
	protected Map<String, ITabularView> executeSubQueries(QueryPod queryPod, Map<String, ICubeQuery> cubeToQuery) {
		Map<String, ICubeWrapper> nameToCube = getNameToCube();

		try {
			// https://stackoverflow.com/questions/21163108/custom-thread-pool-in-java-8-parallel-stream
			return queryPod.getExecutorService().submit(() -> {
				Stream<Entry<String, ICubeQuery>> stream = cubeToQuery.entrySet().stream();

				if (StandardQueryOptions.CONCURRENT.isActive(queryPod.getOptions())) {
					stream = stream.parallel();
				}

				return stream.collect(Collectors.toMap(Entry::getKey, cubeAndQuery -> {
					String cubeName = cubeAndQuery.getKey();
					ICubeWrapper subCube = nameToCube.get(cubeName);
					ICubeQuery query = cubeAndQuery.getValue();
					try {
						return executeSubQuery(subCube, query);
					} catch (RuntimeException e) {
						throw new IllegalArgumentException("Issue querying %s with %s".formatted(cubeName, query), e);
					}
				}));

			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted on cube=%s".formatted(queryPod.getTable().getName()), e);
		} catch (ExecutionException e) {
			throw new IllegalStateException("Failed on cube=%s".formatted(queryPod.getTable().getName()), e);
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
		IAdhocSlice groupBys;
		if (missingColumns.isEmpty()) {
			groupBys = slice;
		} else {
			Map<String, Object> groupBysTmp = new LinkedHashMap<>(slice.getCoordinates());
			missingColumns.forEach(column -> groupBysTmp.put(column, missingColumn(cube, column)));
			groupBys = SliceAsMap.fromMap(groupBysTmp);
		}

		return TabularRecordOverMaps.builder().aggregates(aggregates).slice(groupBys).build();
	}

	protected Object missingColumn(ICubeWrapper cube, String column) {
		return columnsManager.onMissingColumn(cube, column);
	}

	/**
	 *
	 * @param subCube
	 * @param filter
	 *            a {@link ICubeQuery} filter
	 * @param isSubColumn
	 *            is the column available in a {@link ICubeWrapper}
	 * @return the equivalent {@link ISliceFilter} given the subset of columns
	 */
	protected ISliceFilter filterForColumns(ICubeWrapper subCube, ISliceFilter filter, Predicate<String> isSubColumn) {
		return SimpleFilterEditor
				.suppressColumn(filter, isSubColumn.negate(), f -> onMissingFilterColumn(subCube, f), Optional.empty());
	}

	protected ISliceFilter onMissingFilterColumn(ICubeWrapper subCube, IColumnFilter columnFilter) {
		Object cubeCoordinateIfMissing = missingColumn(subCube, columnFilter.getColumn());

		if (columnFilter.getValueMatcher().match(cubeCoordinateIfMissing)) {
			// The filter matches the default coordinate: it matches the whole subCube
			return ISliceFilter.MATCH_ALL;
		} else {
			return ISliceFilter.MATCH_NONE;
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

		builder.measures(measuresToAdd);

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

	@Override
	public Map<String, ?> getHealthDetails() {
		Map<String, Object> details = new LinkedHashMap<>();

		cubes.forEach(cube -> {
			details.put("subCube." + cube.getName(), CubeWrapper.makeDetails(cube));
		});

		return details;
	}

}
