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
package eu.solven.adhoc.engine.tabular;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.google.common.collect.Sets;

import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.ISinkExecutionFeedback;
import eu.solven.adhoc.engine.QueryEngineConcurrencyHelper;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryOptimizer.SplitTableQueries;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.aggregation.collection.UnionSetAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.FilterMatcher;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByHelpers;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocBlackHole;
import eu.solven.adhoc.util.IAdhocEventBus;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds the execution logic related with an {@link ITableQueryEngine}, given a {@link ITableQueryOptimizer}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
@SuppressWarnings("PMD.GodClass")
public class TableQueryEngineBootstrapped {

	@NonNull
	@Default
	final AdhocFactories factories = AdhocFactories.builder().build();

	@NonNull
	@Default
	final IAdhocEventBus eventBus = AdhocBlackHole.getInstance();

	@NonNull
	final ITableQueryOptimizer optimizer;

	public Map<CubeQueryStep, ISliceToValue> executeTableQueries(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		// Collect the tableQueries given the cubeQueryStep, essentially by focusing on aggregated measures
		Set<TableQuery> tableQueries = prepareForTable(queryPod, queryStepsDag);

		// Split these queries given inducing logic. (e.g. `SUM(a) GROUP BY b` may be induced by `SUM(a) GROUP BY b, c`)
		SplitTableQueries inducerAndInduced = optimizer.splitInduced(queryPod, tableQueries);

		logInducedSteps(queryPod, inducerAndInduced);

		// Given the inducers, group them by groupBy, to leverage FILTER per measure
		Set<TableQueryV2> tableQueriesV2 = groupByEnablingFilterPerMeasure(optimizer, inducerAndInduced.getInducers());

		sanityChecks(queryStepsDag, tableQueriesV2);

		// Execute the actual tableQueries
		Map<CubeQueryStep, ISliceToValue> stepToValues = executeTableQueries(queryPod, queryStepsDag, tableQueriesV2);

		// Evaluated the induced tableQueries
		walkUpInducedDag(queryPod, stepToValues, inducerAndInduced);

		reportAfterTableQueries(queryPod, stepToValues);

		return stepToValues;
	}

	protected void logInducedSteps(QueryPod queryPod, SplitTableQueries inducerAndInduced) {
		if (queryPod.isDebugOrExplain()) {
			DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = inducerAndInduced.getDagToDependancies();
			log.info("[EXPLAIN] About to show {} inducers steps leading to {} induced steps",
					inducerAndInduced.getInducers().size(),
					inducerAndInduced.getInduceds().size());

			// TODO We want to print the graph of induced steps. There should be something to refactor with DagExplainer
			// new TopologicalOrderIterator<>(inducerAndInduced.getDagToDependancies()).fo
			dag.edgeSet().forEach(edge -> {
				CubeQueryStep inducer = dag.getEdgeSource(edge);
				CubeQueryStep induced = dag.getEdgeTarget(edge);
				log.info("[EXPLAIN] {} will induce {}", inducer, induced);
			});
		}
	}

	protected void reportAfterTableQueries(QueryPod queryPod,
			Map<CubeQueryStep, ISliceToValue> queryStepToValuesOuter) {
		if (queryPod.isDebug()) {
			queryStepToValuesOuter.forEach((queryStep, values) -> {
				values.forEachSlice(row -> {
					return rowValue -> {
						eventBus.post(AdhocLogEvent.builder()
								.debug(true)
								.message("%s -> %s".formatted(rowValue, row))
								.source(queryStep)
								.build());
					};
				});

			});
		}
	}

	// Manages concurrency: the logic here should be strictly minimal on-top of concurrency
	protected Map<CubeQueryStep, ISliceToValue> executeTableQueries(QueryPod queryPod,
			ISinkExecutionFeedback sinkExecutionFeedback,
			Set<TableQueryV2> tableQueries) {
		try {
			return queryPod.getExecutorService().submit(() -> {
				Stream<TableQueryV2> tableQueriesStream = tableQueries.stream();

				if (queryPod.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
					tableQueriesStream = tableQueriesStream.parallel();
				}
				Map<CubeQueryStep, ISliceToValue> queryStepToValuesInner = new ConcurrentHashMap<>();
				tableQueriesStream.forEach(tableQuery -> {
					Map<CubeQueryStep, ISliceToValue> queryStepToValues =
							processOneTableQuery(queryPod, sinkExecutionFeedback, tableQuery);

					queryStepToValuesInner.putAll(queryStepToValues);
				});

				return queryStepToValuesInner;
			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted", e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof IllegalStateException) {
				throw new IllegalStateException("Failed", e);
			} else {
				throw new IllegalArgumentException("Failed", e);
			}
		}
	}

	protected Map<CubeQueryStep, ISliceToValue> processOneTableQuery(QueryPod queryPod,
			ISinkExecutionFeedback sinkExecutionFeedback,
			TableQueryV2 dagQuery) {
		TableQueryV2 suppressedQuery = suppressGeneratedColumns(queryPod, dagQuery);

		// TODO We may be querying multiple times the same suppressedTableQuery
		// e.g. if 2 tableQueries differs only by a suppressedColumn
		TableQueryToSuppressedTableQuery toSuppressed =
				TableQueryToSuppressedTableQuery.builder().dagQuery(dagQuery).suppressedQuery(suppressedQuery).build();

		IStopwatch stopWatchSinking;

		Map<CubeQueryStep, ISliceToValue> stepToValues;

		IStopwatch openingStopwatch = factories.getStopwatchFactory().createStarted();
		// Open the stream: the table may or may not return after the actual execution
		try (ITabularRecordStream rowsStream = openTableStream(queryPod, suppressedQuery)) {
			if (queryPod.isExplain() || queryPod.isDebug()) {
				// JooQ may be slow to load some classes
				// Slowness also due to fetching stream characteristics, which actually open the query
				Duration openingElasped = openingStopwatch.elapsed();
				eventBus.post(AdhocLogEvent.builder()
						.debug(queryPod.isDebug())
						.explain(queryPod.isExplain())
						.performance(true)
						.message("time=%s for openingStream on %s"
								.formatted(PepperLogHelper.humanDuration(openingElasped.toMillis()), dagQuery))
						.source(this)
						.build());
			}

			stopWatchSinking = factories.getStopwatchFactory().createStarted();
			stepToValues = aggregateStreamToAggregates(queryPod, toSuppressed, rowsStream);
		}

		Duration elapsed = stopWatchSinking.elapsed();
		reportAboutDoneAggregators(sinkExecutionFeedback, elapsed, stepToValues);

		return stepToValues;
	}

	protected void reportAboutDoneAggregators(ISinkExecutionFeedback sinkExecutionFeedback,
			Duration elapsed,
			Map<CubeQueryStep, ISliceToValue> oneQueryStepToValues) {
		oneQueryStepToValues.forEach((queryStep, column) -> {
			eventBus.post(QueryStepIsCompleted.builder()
					.querystep(queryStep)
					.nbCells(column.size())
					// The duration is not decomposed per aggregator
					.duration(elapsed)
					.source(this)
					.build());

			sinkExecutionFeedback.registerExecutionFeedback(queryStep,
					SizeAndDuration.builder().size(column.size()).duration(elapsed).build());
		});
	}

	/**
	 * @param queryPod
	 * @param queryStepsDag
	 * @return the Set of {@link TableQuery} to be executed.
	 */
	protected Set<TableQuery> prepareForTable(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		// Pack each steps targeting the same groupBy+filters. Multiple measures can be evaluated on such packs.
		Map<MeasurelessQuery, Set<Aggregator>> measurelessToAggregators = new LinkedHashMap<>();

		// https://stackoverflow.com/questions/57134161/how-to-find-roots-and-leaves-set-in-jgrapht-directedacyclicgraph
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = queryStepsDag.getDag();
		dag.vertexSet()
				.stream()
				// Consider only leaves steps.
				// BEWARE We could filter for `Aggregator` but it may be relevant to behave specifically on
				// `Transformator` with no undelryingSteps
				.filter(step -> dag.outgoingEdgesOf(step).isEmpty())
				// Skip steps with a value in cache
				.filter(step -> !queryStepsDag.getStepToValues().containsKey(step))
				.forEach(step -> {
					IMeasure leafMeasure = queryPod.resolveIfRef(step.getMeasure());

					if (leafMeasure instanceof Aggregator leafAggregator) {
						MeasurelessQuery measureless = MeasurelessQuery.edit(step).build();

						// Aggregator leaves are groupedBy context (groupBy+filter+customMarker)
						// They may be later grouped by different granularities (e.g. to leverage `FILTER` per measure)
						measurelessToAggregators
								.merge(measureless, Set.of(leafAggregator), UnionSetAggregation::unionSet);
					} else if (leafMeasure instanceof EmptyMeasure) {
						log.trace("An EmptyMeasure has no underlying measures");
					} else {
						// Happens on Transformator with no underlying queryStep (no underlying measure, or planned as
						// having no underlying step (e.g. Filtrator with matchNone filter))
						log.debug("step={} has been planned having no underlying step", step);
					}
				});

		return measurelessToAggregators.entrySet().stream().map(e -> {
			MeasurelessQuery measurelessQuery = e.getKey();
			Set<Aggregator> leafAggregators = e.getValue();
			return TableQuery.edit(measurelessQuery).aggregators(leafAggregators).build();
		}).collect(Collectors.toCollection(LinkedHashSet::new));
	}

	/**
	 * This step handles columns which are not relevant for the tables, but has not been suppressed by the measure tree.
	 * It typically happens when a column is introduced by some {@link Dispatchor} measure, but the actually requested
	 * measure is unrelated (which itself happens when selecting multiple measures, like `many2many+count(*)`).
	 * 
	 * @param queryPod
	 * 
	 * @param tableQuery
	 * @return {@link TableQuery} where calculated columns has been suppressed.
	 */
	protected TableQueryV2 suppressGeneratedColumns(QueryPod queryPod, TableQueryV2 tableQuery) {
		// We list the generatedColumns instead of listing the table columns as many tables has lax resolution of
		// columns (e.g. given a joined `tableName.fieldName`, `fieldName` is a valid columnName. `.getColumns` would
		// probably return only one of the 2).
		Set<String> generatedColumns = queryPod.getColumnsManager()
				.getGeneratedColumns(factories.getOperatorFactory(),
						queryPod.getForest().getMeasures(),
						IValueMatcher.MATCH_ALL)
				.stream()
				.flatMap(cg -> cg.getColumnTypes().keySet().stream())
				.collect(Collectors.toSet());

		Set<String> groupedByCubeColumns = tableQuery.getGroupBy().getGroupedByColumns();

		var edited = tableQuery.toBuilder();

		Set<String> generatedColumnToSuppressFromGroupBy = Sets.intersection(groupedByCubeColumns, generatedColumns);
		if (!generatedColumnToSuppressFromGroupBy.isEmpty()) {
			// All columns has been validated as being generated
			IAdhocGroupBy originalGroupby = tableQuery.getGroupBy();
			IAdhocGroupBy suppressedGroupby =
					GroupByHelpers.suppressColumns(originalGroupby, generatedColumnToSuppressFromGroupBy);
			if (queryPod.isDebugOrExplain()) {
				eventBus.post(AdhocLogEvent.builder()
						.debug(queryPod.isDebug())
						.explain(queryPod.isExplain())
						.source(this)
						.messageT("Suppressing generatedColumns={} in groupBy from {} to {}",
								generatedColumnToSuppressFromGroupBy,
								originalGroupby,
								suppressedGroupby)
						.build());
			} else {
				log.debug("Suppressing generatedColumns={} in groupBy from {} to {}",
						generatedColumnToSuppressFromGroupBy,
						originalGroupby,
						suppressedGroupby);
			}
			edited.groupBy(suppressedGroupby);
		}

		Set<String> filteredCubeColumnsToSuppress =
				FilterHelpers.getFilteredColumns(tableQuery.getFilter()).stream().collect(Collectors.toSet());
		Set<String> generatedColumnToSuppressFromFilter =
				Sets.intersection(filteredCubeColumnsToSuppress, generatedColumns);

		if (!generatedColumnToSuppressFromFilter.isEmpty()) {
			ISliceFilter originalFilter = tableQuery.getFilter();
			ISliceFilter suppressedFilter =
					SimpleFilterEditor.suppressColumn(originalFilter, generatedColumnToSuppressFromFilter);

			if (queryPod.isDebugOrExplain()) {
				eventBus.post(AdhocLogEvent.builder()
						.debug(queryPod.isDebug())
						.explain(queryPod.isExplain())
						.source(this)
						.messageT("Suppressing generatedColumns in filter from {} to {}",
								originalFilter,
								suppressedFilter)
						.build());
			} else {
				log.debug("Suppressing generatedColumns in filter from {} to {}", originalFilter, suppressedFilter);
			}
			edited.filter(suppressedFilter);
		}

		return edited.build();
	}

	protected ITabularRecordStream openTableStream(QueryPod queryPod, TableQueryV2 tableQuery) {
		return queryPod.getColumnsManager().openTableStream(queryPod, tableQuery);
	}

	protected Map<CubeQueryStep, ISliceToValue> aggregateStreamToAggregates(QueryPod queryPod,
			TableQueryToSuppressedTableQuery queryAndSuppressed,
			ITabularRecordStream stream) {

		IMultitypeMergeableGrid<IAdhocSlice> sliceToAggregates =
				mergeTableAggregates(queryPod, queryAndSuppressed, stream);

		Map<CubeQueryStep, ISliceToValue> immutableChunks =
				splitTableGridToColumns(queryPod, queryAndSuppressed, sliceToAggregates);
		return immutableChunks;
	}

	protected Map<CubeQueryStep, ISliceToValue> splitTableGridToColumns(QueryPod queryPod,
			TableQueryToSuppressedTableQuery queryAndSuppressed,
			IMultitypeMergeableGrid<IAdhocSlice> sliceToAggregates) {
		IStopwatch singToAggregatedStarted = factories.getStopwatchFactory().createStarted();

		Map<CubeQueryStep, ISliceToValue> immutableChunks =
				toSortedColumns(queryPod, queryAndSuppressed, sliceToAggregates);

		// BEWARE This timing is independent of the table
		Duration elapsed = singToAggregatedStarted.elapsed();
		if (queryPod.isDebug() || queryPod.isExplain()) {
			long[] sizes = immutableChunks.values().stream().mapToLong(ISliceToValue::size).toArray();

			if (queryPod.isDebug()) {
				long totalSize = immutableChunks.values().stream().mapToLong(ISliceToValue::size).sum();

				eventBus.post(AdhocLogEvent.builder()
						.debug(true)
						.performance(true)
						.message("time=%s sizes=%s total_size=%s for toSortedColumns on %s".formatted(
								PepperLogHelper.humanDuration(elapsed.toMillis()),
								Arrays.toString(sizes),
								totalSize,
								queryAndSuppressed.getDagQuery()))
						.source(this)
						.build());
			} else if (queryPod.isExplain()) {
				eventBus.post(AdhocLogEvent.builder()
						.explain(true)
						.performance(true)
						.message("time=%s sizes=%s for toSortedColumns on %s".formatted(
								PepperLogHelper.humanDuration(elapsed.toMillis()),
								Arrays.toString(sizes),
								queryAndSuppressed.getDagQuery()))
						.source(this)
						.build());
			}
		}
		return immutableChunks;
	}

	protected IMultitypeMergeableGrid<IAdhocSlice> mergeTableAggregates(QueryPod queryPod,
			TableQueryToSuppressedTableQuery queryAndSuppressed,
			ITabularRecordStream stream) {
		IStopwatch stopWatch = factories.getStopwatchFactory().createStarted();

		IMultitypeMergeableGrid<IAdhocSlice> sliceToAggregates =
				mergeTableAggregates(queryPod, queryAndSuppressed.getSuppressedQuery(), stream);

		// BEWARE This timing is dependent of the table
		Duration elapsed = stopWatch.elapsed();
		TableQueryV2 dagQuery = queryAndSuppressed.getDagQuery();
		if (queryPod.isDebug()) {
			long totalSize = dagQuery.getAggregators().stream().mapToLong(sliceToAggregates::size).sum();

			eventBus.post(AdhocLogEvent.builder()
					.debug(true)
					.performance(true)
					.message("time=%s size=%s for mergeTableAggregates on %s"
							.formatted(PepperLogHelper.humanDuration(elapsed.toMillis()), totalSize, dagQuery))
					.source(this)
					.build());
		} else if (queryPod.isExplain()) {
			eventBus.post(AdhocLogEvent.builder()
					.explain(true)
					.performance(true)
					.message("time=%s for mergeTableAggregates on %s"
							.formatted(PepperLogHelper.humanDuration(elapsed.toMillis()), dagQuery))
					.source(this)
					.build());
		}
		return sliceToAggregates;
	}

	protected IMultitypeMergeableGrid<IAdhocSlice> mergeTableAggregates(QueryPod queryPod,
			TableQueryV2 tableQuery,
			ITabularRecordStream stream) {
		ITabularRecordStreamReducer streamReducer = makeTabularRecordStreamReducer(queryPod, tableQuery);

		return streamReducer.reduce(stream);
	}

	protected ITabularRecordStreamReducer makeTabularRecordStreamReducer(QueryPod queryPod, TableQueryV2 tableQuery) {
		return TabularRecordStreamReducer.builder()
				.operatorFactory(factories.getOperatorFactory())
				.sliceFactory(factories.getSliceFactory())
				.queryPod(queryPod)
				.tableQuery(tableQuery)
				.build();
	}

	protected Optional<Aggregator> optAggregator(Map<String, Set<Aggregator>> columnToAggregators,
			String aggregatorName) {
		return columnToAggregators.values()
				.stream()
				.flatMap(Collection::stream)
				.filter(a -> a.getName().equals(aggregatorName))
				.findAny();
	}

	/**
	 * 
	 * @param queryPod
	 * @param query
	 * @param coordinatesToAggregates
	 * @return a {@link Map} from each {@link Aggregator} to the column of values
	 */
	protected Map<CubeQueryStep, ISliceToValue> toSortedColumns(QueryPod queryPod,
			TableQueryToSuppressedTableQuery query,
			IMultitypeMergeableGrid<IAdhocSlice> coordinatesToAggregates) {
		Map<CubeQueryStep, ISliceToValue> queryStepToValues = new LinkedHashMap<>();
		TableQueryV2 dagTableQuery = query.getDagQuery();

		Set<String> suppressedGroupBys = query.getSuppressedGroupBy();

		dagTableQuery.getAggregators().forEach(filteredAggregator -> {
			CubeQueryStep queryStep = recombineQueryStep(dagTableQuery, filteredAggregator);

			// `.closeColumn` may be an expensive operation. e.g. it may sort slices.
			IMultitypeColumnFastGet<IAdhocSlice> values = coordinatesToAggregates.closeColumn(filteredAggregator);

			IMultitypeColumnFastGet<IAdhocSlice> valuesWithSuppressed;
			if (suppressedGroupBys.isEmpty()) {
				valuesWithSuppressed = values;
			} else {
				valuesWithSuppressed = restoreSuppressedGroupBy(queryStep, suppressedGroupBys, values);
			}

			// The aggregation step is done: the storage is supposed not to be edited: we
			// re-use it in place, to spare a copy to an immutable container
			queryStepToValues.put(queryStep, SliceToValue.forGroupBy(queryStep).values(valuesWithSuppressed).build());
		});
		return queryStepToValues;
	}

	protected CubeQueryStep recombineQueryStep(TableQueryV2 dagTableQuery, FilteredAggregator filteredAggregator) {
		Aggregator aggregator = filteredAggregator.getAggregator();

		return CubeQueryStep.edit(dagTableQuery)
				// Recombine the stepFilter given the tableQuery filter and the measure filter
				// BEWARE as queryStep is used as key, it is primordial `AndFilter.and(...)` is equal to the
				// original filter, which may be false in case of some optimization in `AndFilter` (e.g. prefering
				// some `!OR`).
				.filter(recombineWhereAndFilter(dagTableQuery, filteredAggregator))
				.measure(aggregator)
				.build();
	}

	protected ISliceFilter recombineWhereAndFilter(TableQueryV2 dagTableQuery, FilteredAggregator filteredAggregator) {
		return AndFilter.and(dagTableQuery.getFilter(), filteredAggregator.getFilter());
	}

	/**
	 * Given a {@link IMultitypeColumnFastGet} from the {@link ITableWrapper}, we may have to add columns which has been
	 * suppressed (e.g. due to IColumnGenerator).
	 * 
	 * Current implementation restore the suppressedColumn by writing a single member. Another project may prefer
	 * writing a constant member (e.g. `suppressed`), or duplicating the value for each possible members of the
	 * suppressed column (through, beware it may lead to a large Cartesian product in case of multiple suppressed
	 * columns).
	 * 
	 * @param queryStep
	 * @param suppressedColumns
	 * @param column
	 * @return
	 */
	protected IMultitypeColumnFastGet<IAdhocSlice> restoreSuppressedGroupBy(CubeQueryStep queryStep,
			Set<String> suppressedColumns,
			IMultitypeColumnFastGet<IAdhocSlice> column) {
		Map<String, ?> constantValues = valuesForSuppressedColumns(suppressedColumns, queryStep);

		boolean match = FilterMatcher.builder().filter(queryStep.getFilter()).onMissingColumn(cf -> {
			// We test only suppressedColumns for now
			// TODO We should actually test each slice individually. It would lead to issues on complex filters (e.g.
			// with OR), and fails around `eu.solven.adhoc.engine.step.SliceAsMapWithStep.asFilter()`.
			return true;
		}).build().match(constantValues);

		if (match) {
			return GroupByHelpers.addConstantColumns(column, constantValues);
		} else {
			log.debug("suppressedColumns={} are filtered out by {}", constantValues, queryStep.getFilter());
			return MultitypeHashColumn.empty();
		}
	}

	/**
	 * 
	 * @param suppressedColumns
	 * @param queryStep
	 *            the queryStep can be used to customize the suppress column values
	 * @return
	 */
	protected Map<String, ?> valuesForSuppressedColumns(Set<String> suppressedColumns, CubeQueryStep queryStep) {
		return suppressedColumns.stream().collect(Collectors.toMap(c -> c, c -> IColumnGenerator.COORDINATE_GENERATED));
	}

	protected Set<TableQueryV2> groupByEnablingFilterPerMeasure(ITableQueryOptimizer tableQueryOptimizer,
			Set<CubeQueryStep> tableQueries) {
		return tableQueryOptimizer.groupByEnablingFilterPerMeasure(tableQueries);
	}

	protected void sanityChecks(QueryStepsDag queryStepsDag, Set<TableQueryV2> tableQueries) {
		tableQueries.forEach(tableQuery -> {
			tableQuery.getAggregators().forEach(aggregator -> {
				CubeQueryStep queryStep = recombineQueryStep(tableQuery, aggregator);

				if (!queryStepsDag.getDag().containsVertex(queryStep)) {
					// This typically happens due to inconsistency in equality if ISliceFiler (e.g. `a` and
					// `Not(Not(a))`)
					throw new IllegalStateException(
							"%s is invalid but generated by %s with %s".formatted(queryStep, aggregator, tableQuery));
				}
			});
		});
	}

	/**
	 * 
	 * @param queryPod
	 * @param stepToValues
	 *            a mutable {@link Map}. May need to be thread-safe.
	 * @param inducerAndInduced
	 */
	protected void walkUpInducedDag(QueryPod queryPod,
			Map<CubeQueryStep, ISliceToValue> stepToValues,
			SplitTableQueries inducerAndInduced) {
		Consumer<? super CubeQueryStep> queryStepConsumer = induced -> {
			try {
				evaluateInduced(queryPod, stepToValues, inducerAndInduced, induced);
			} catch (RuntimeException e) {
				throw AdhocExceptionHelpers.wrap(e, "Issue inducing step=%s".formatted(induced));
			}
		};

		QueryEngineConcurrencyHelper.walkUpDag(queryPod, inducerAndInduced, stepToValues, queryStepConsumer);
	}

	protected void evaluateInduced(IHasQueryOptions hasOptions,
			Map<CubeQueryStep, ISliceToValue> stepToValues,
			SplitTableQueries inducerAndInduced,
			CubeQueryStep induced) {
		if (stepToValues.containsKey(induced)) {
			// Happens typically for inducers steps
			log.debug("step={} is already evaluated", induced);
		} else if (inducerAndInduced.getInducers().contains(induced)) {
			// Typically happen if `a` is not equals to `not(not(a))`
			// Would relate with `recombineWhereAndFilter`

			// BEWARE Showing all available steps to help investigation. This may be a huge log, but it is helpful as
			// such issue is difficult to investigate
			String availableSteps = stepToValues.keySet()
					.stream()
					.map(Object::toString)
					.collect(Collectors.joining(System.lineSeparator()));

			throw new IllegalStateException(
					"inducer=%s is missing its value-column. May happen on .equals inconsistency in ISliceFilter. Steps are: {}{}"
							.formatted(induced, System.lineSeparator(), availableSteps));
		} else {
			IMultitypeMergeableColumn<IAdhocSlice> inducedValues =
					optimizer.evaluateInduced(hasOptions, inducerAndInduced, stepToValues, induced);

			stepToValues.put(induced, SliceToValue.forGroupBy(induced).values(inducedValues).build());
		}
	}
}