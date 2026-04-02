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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.MultimapBuilder.SetMultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.column.ColumnMetadata.ColumnMetadataBuilder;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.dataframe.row.TabularRecordOverMaps;
import eu.solven.adhoc.dataframe.stream.IConsumingStream;
import eu.solven.adhoc.dataframe.stream.SuppliedTabularRecordConsumingStream;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.observability.IHasHealthDetails;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.IColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.MeasureHelpers;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.IHasAggregationKey;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.TableWrapperHelpers;
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
	public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV4 compositeQuery) {
		return TableWrapperHelpers.v3TovV2(queryPod, compositeQuery.streamV3(), this);
	}

	@Override
	public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV2 compositeQuery) {
		if (!Objects.equals(this, queryPod.getTable())) {
			throw new IllegalStateException("Inconsistent tables: %s vs %s".formatted(queryPod.getTable(), this));
		}

		checkColumns(compositeQuery);

		Map<String, ICubeQuery> cubeToQuery = new LinkedHashMap<>();

		cubes.stream().filter(subCube -> isEligible(subCube, compositeQuery)).forEach(subCube -> {
			ICubeQuery subQuery = makeSubQuery(queryPod, compositeQuery, subCube);

			var previous = cubeToQuery.put(subCube.getName(), subQuery);
			if (previous != null) {
				throw new IllegalStateException("Multiple cubes are named: " + subCube.getName());
			}
		});

		// Actual execution is the only concurrent section
		final Map<String, ITabularView> cubeToView = executeSubQueries(queryPod, cubeToQuery);

		// not distinct slices as different subCubes may refer to the same slices
		return new SuppliedTabularRecordConsumingStream(compositeQuery,
				false,
				() -> IConsumingStream.fromStream(openStream(compositeQuery, cubeToView)));
	}

	/**
	 * This method will check the columns in the compositeQuery are valid. This is done early as in later phase, each
	 * subCube will discard unknown columns, supposing another cube will take it in charge.
	 * 
	 * @param compositeQuery
	 */
	protected void checkColumns(TableQueryV2 compositeQuery) {
		NavigableSet<String> groupedByColumns =
				new ConcurrentSkipListSet<>(compositeQuery.getGroupBy().getSortedColumns());
		Set<String> filteredColumns =
				new ConcurrentSkipListSet<>(FilterHelpers.getFilteredColumns(compositeQuery.getFilter()));

		optCubeSlicer.ifPresent(cubeSlicer -> {
			groupedByColumns.remove(cubeSlicer);
			filteredColumns.remove(cubeSlicer);
		});

		Stream<ICubeWrapper> cubeStream = cubes.stream();

		// `.getColumnsAsMap` can be slow, so we consider concurrency
		if (StandardQueryOptions.CONCURRENT.isActive(compositeQuery.getOptions())) {
			cubeStream = cubeStream.parallel();
		}

		// Due to concurrency, we can not stop early as soon as we confirmed all columns as known by at least one
		// subCube
		cubeStream.forEach(cube -> {
			Set<String> cubeColumns = cube.getColumnsAsMap().keySet();
			groupedByColumns.removeAll(cubeColumns);
			filteredColumns.removeAll(cubeColumns);
		});

		if (!groupedByColumns.isEmpty()) {
			throw new IllegalArgumentException(
					"unknown groupedBy columns: %s for query=%s".formatted(groupedByColumns, compositeQuery));
		} else if (!filteredColumns.isEmpty()) {
			throw new IllegalArgumentException(
					"unknown filtered columns: %s for query=%s".formatted(filteredColumns, compositeQuery));
		}

	}

	protected Stream<ITabularRecord> openStream(TableQueryV2 compositeQuery,
			final Map<String, ITabularView> cubeToView) {
		Map<String, ICubeWrapper> nameToCube = getNameToCube();

		return cubeToView.entrySet().stream().flatMap(e -> {
			ICubeWrapper subCube = nameToCube.get(e.getKey());
			Set<String> subColumns = subCube.getColumnsAsMap().keySet();

			// Columns which are requested (hence present in the composite Cube/ one of the subCube) but missing
			// from current subCube.
			NavigableSet<String> subMissingColumns =
					new TreeSet<>(Sets.difference(compositeQuery.getGroupBy().getSortedColumns(), subColumns));

			Map<String, Object> missingColumnsAsmask;

			if (subMissingColumns.isEmpty()) {
				missingColumnsAsmask = Map.of();
			} else {
				missingColumnsAsmask = LinkedHashMap.newLinkedHashMap(subMissingColumns.size());
				subMissingColumns.forEach(column -> missingColumnsAsmask.put(column, missingColumn(subCube, column)));
			}

			ITabularView subView = e.getValue();
			return subView.stream(slice -> {
				return oAsMap -> {
					return transcodeSliceToComposite(compositeQuery
							.getGroupBy(), subCube, slice, oAsMap, missingColumnsAsmask);
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
						return MeasureHelpers.alias(fa.getAlias(), fa.getAggregator().getColumnName());
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

	protected ICubeQuery makeSubQuery(QueryPod queryPod, TableQueryV2 compositeQuery, ICubeWrapper subCube) {
		Predicate<String> subCubeKnownMeasure = makeSubColumnPredicate(subCube);

		// groupBy only by relevant columns. Other columns are ignored
		NavigableMap<String, IAdhocColumn> subGroupBy = new TreeMap<>();

		compositeQuery.getGroupBys()
				.forEach(compositeGroupBy -> subGroupBy.putAll(compositeGroupBy.getSortedNameToColumn()));

		subGroupBy.keySet().removeIf(Predicate.not(subCubeKnownMeasure::test));

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

	/**
	 * Executes all sub-queries and returns their results keyed by cube name in insertion order.
	 * <p>
	 * {@link CompositeCubesTableWrapper} is an in-process fan-out. It uses {@link QueryPod#getExecutorService()} which
	 * is a Virtual Thread executor when the query is concurrent, so all sub-queries can run in parallel without
	 * exhausting platform threads.
	 */
	@SuppressWarnings({ "PMD.CloseResource", "PMD.ExceptionAsFlowControl" })
	protected Map<String, ITabularView> executeSubQueries(QueryPod queryPod, Map<String, ICubeQuery> cubeToQuery) {
		Map<String, ICubeWrapper> nameToCube = getNameToCube();

		try {
			if (!StandardQueryOptions.CONCURRENT.isActive(queryPod.getOptions())) {
				// Sequential path: run every sub-query on the calling thread
				Map<String, ITabularView> result = new LinkedHashMap<>();
				cubeToQuery.forEach((cubeName, query) -> {
					ICubeWrapper subCube = nameToCube.get(cubeName);
					try {
						result.put(cubeName, executeSubQuery(subCube, query));
					} catch (RuntimeException e) {
						throw new IllegalArgumentException("Issue querying %s with %s".formatted(cubeName, query), e);
					}
				});
				return result;
			} else {
				// Concurrent path: submit to the CPU pool (not the DB/I-O pool) to avoid pool re-entrancy deadlock
				ListeningExecutorService cpuPool = queryPod.getExecutorService();
				Map<String, CompletableFuture<ITabularView>> futures = new LinkedHashMap<>();
				cubeToQuery.forEach((cubeName, query) -> {
					ICubeWrapper subCube = nameToCube.get(cubeName);
					futures.put(cubeName, CompletableFuture.supplyAsync(() -> {
						try {
							return executeSubQuery(subCube, query);
						} catch (RuntimeException e) {
							throw new IllegalArgumentException("Issue querying %s with %s".formatted(cubeName, query),
									e);
						}
					}, cpuPool));
				});
				Map<String, ITabularView> result = new LinkedHashMap<>();
				futures.forEach((cubeName, future) -> result.put(cubeName, future.join()));
				return result;
			}
		} catch (RuntimeException e) {
			throw AdhocExceptionHelpers.wrap("Issue querying table=%s".formatted(queryPod.getTable().getName()), e);
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
	 * @param missingColumnsMask
	 *            the columns in the compositeQuery groupBy, missing in the underlying cube
	 * @return
	 */
	protected ITabularRecord transcodeSliceToComposite(IGroupBy groupBy,
			ICubeWrapper cube,
			ISlice slice,
			Map<String, ?> measures,
			Map<String, ?> missingColumnsMask) {
		ISlice compositeSlice;
		if (missingColumnsMask.isEmpty()) {
			compositeSlice = slice;
		} else {
			compositeSlice = slice.addColumns(missingColumnsMask);
		}

		return TabularRecordOverMaps.builder().aggregates(measures).slice(groupBy, compositeSlice).build();
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
		Set<String> compositeMeasures = new LinkedHashSet<>(compositeForest.getNameToMeasure().keySet());

		SetMultimap<String, String> measureToCubes = HashMultimap.create();

		Set<IMeasure> measuresToAdd = new LinkedHashSet<>();

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
