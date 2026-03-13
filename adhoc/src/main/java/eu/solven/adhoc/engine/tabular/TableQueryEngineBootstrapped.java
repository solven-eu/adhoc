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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.collection.AdhocCollectionHelpers;
import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.data.column.Cuboid;
import eu.solven.adhoc.data.column.ICuboid;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.ISinkExecutionFeedback;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.concurrent.QueryEngineConcurrencyHelper;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.observability.DagExplainer;
import eu.solven.adhoc.engine.observability.DagExplainerForPerfs;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.inducer.ITableQueryInducer;
import eu.solven.adhoc.engine.tabular.optimizer.IHasDagFromInducedToInducer;
import eu.solven.adhoc.engine.tabular.optimizer.IHasFilterOptimizer;
import eu.solven.adhoc.engine.tabular.optimizer.IHasTableQueryForSteps;
import eu.solven.adhoc.engine.tabular.optimizer.IHasTableQueryForSteps.StepAndFilteredAggregator;
import eu.solven.adhoc.engine.tabular.optimizer.ITableQueryFactory;
import eu.solven.adhoc.engine.tabular.optimizer.SplitTableQueries;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.eventbus.TableStepIsCompleted;
import eu.solven.adhoc.eventbus.TableStepIsEvaluating;
import eu.solven.adhoc.eventbus.UnsafeAdhocEventBusHelpers;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.FilterMatcher;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByHelpers;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocBlackHole;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.core.PepperStreamHelperHacked;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds the execution logic related with an {@link ITableQueryEngine}, given a {@link ITableQueryFactory} in the
 * context of a single {@link TableQuery}.
 * 
 * @author Benoit Lacelle
 * @see TableQueryEngine
 */
@Slf4j
@Builder
@SuppressWarnings("PMD.GodClass")
// https://math.stackexchange.com/questions/2966359/how-to-calculate-cost-in-discrete-markov-transitions
public class TableQueryEngineBootstrapped implements ITableQueryEngineBootstrapped {

	@NonNull
	@Default
	@Getter(AccessLevel.PRIVATE)
	final IAdhocFactories factories = AdhocFactories.builder().build();

	@NonNull
	@Default
	final IAdhocEventBus eventBus = UnsafeAdhocEventBusHelpers.safeWrapper(AdhocBlackHole.getInstance());

	@NonNull
	final QueryPod queryPod;

	@NonNull
	@Getter(AccessLevel.PRIVATE)
	final ITableQueryFactory optimizer;

	final ITableQueryInducer inducer;

	final Supplier<IFilterOptimizer> filterOptimizerSupplier = Suppliers.memoize(() -> {
		if (getOptimizer() instanceof IHasFilterOptimizer hasFilterOptimizer) {
			// Most ITableQueryOptimizer has a filterOptimizerWithCache
			return hasFilterOptimizer.getFilterOptimizer();
		} else {
			return this.getFactories().getFilterOptimizerFactory().makeOptimizerWithCache();
		}
	});

	@Override
	public Map<CubeQueryStep, ICuboid> executeTableQueries(QueryStepsDag queryStepsDag) {
		ISinkExecutionFeedback executionFeedfack = prepareExecutionFeedback(queryStepsDag);

		// Collect the tableQueries given the cubeQueryStep, essentially by focusing on aggregated measures
		Set<CubeQueryStep> tableQuerySteps = prepareForTable(queryStepsDag);

		Map<CubeQueryStep, CubeQueryStep> calculatedToSuppressed = new LinkedHashMap<>();

		tableQuerySteps.forEach(generatedStep -> {
			calculatedToSuppressed.put(generatedStep, suppressGeneratedColumns(generatedStep));
		});

		Set<CubeQueryStep> suppressedQuerySteps = ImmutableSet.copyOf(calculatedToSuppressed.values());
		log.debug("From {} generated to {} suppressed", calculatedToSuppressed.size(), suppressedQuerySteps.size());

		Map<CubeQueryStep, ICuboid> stepToSuppressedValues =
				executeTableQueries(suppressedQuerySteps, executionFeedfack);

		return restoreSuppressedGroupBy(calculatedToSuppressed, stepToSuppressedValues);
	}

	private ISinkExecutionFeedback prepareExecutionFeedback(QueryStepsDag queryStepsDag) {
		// This is probably bad design
		// Needed to transfer the explicitNodes from tableSteps into the rootNodes in cubeSteps
		return (queryStep, sizeAndDuration) -> {
			if (queryStepsDag.getMultigraph().containsVertex(queryStep)) {
				queryStepsDag.registerExecutionFeedback(queryStep, sizeAndDuration);
			}
		};
	}

	protected Map<CubeQueryStep, ICuboid> restoreSuppressedGroupBy(
			Map<CubeQueryStep, CubeQueryStep> calculatedToSuppressed,
			Map<CubeQueryStep, ICuboid> stepToSuppressedValues) {
		SetMultimap<CubeQueryStep, CubeQueryStep> asMultimap = Multimaps.forMap(calculatedToSuppressed);
		SetMultimap<CubeQueryStep, CubeQueryStep> suppressedToGenerated =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();
		Multimaps.invertFrom(asMultimap, suppressedToGenerated);

		Map<CubeQueryStep, ICuboid> stepToValues = new LinkedHashMap<>();
		{
			stepToSuppressedValues.forEach((suppressedStep, suppressedValue) -> {
				Set<CubeQueryStep> generated = suppressedToGenerated.get(suppressedStep);

				generated.forEach(oneGenerated -> {
					Set<String> suppressedGroupBys =
							getSuppressedGroupBy(oneGenerated.getGroupBy(), suppressedStep.getGroupBy());

					ICuboid generatedValue;
					if (suppressedGroupBys.isEmpty()) {
						generatedValue = suppressedValue;
					} else {
						generatedValue = restoreSuppressedGroupBy(oneGenerated, suppressedGroupBys, suppressedValue);
					}
					stepToValues.put(oneGenerated, generatedValue);
				});

			});
		}
		return stepToValues;
	}

	protected Map<CubeQueryStep, ICuboid> executeTableQueries(Set<CubeQueryStep> steps,
			ISinkExecutionFeedback executionFeedfack) {
		// Split these queries given inducing logic. (e.g. `SUM(a) GROUP BY b` may be induced by `SUM(a) GROUP BY b, c`)
		SplitTableQueries inducerAndInduced = optimizer.splitInduced(queryPod, steps);

		// Execute the actual tableQueries
		Map<CubeQueryStep, ICuboid> stepToSuppressedValues = executeTableQueries(inducerAndInduced, inducerAndInduced);

		QueryPod tableQueryPod = queryPod.asTableQuery();

		{
			if (queryPod.isDebugOrExplain()) {
				explainDagSteps(tableQueryPod, inducerAndInduced);
			}

			// Evaluated the induced tableQueries
			walkUpInducedDag(stepToSuppressedValues, inducerAndInduced);

			if (queryPod.isDebugOrExplain()) {
				explainDagPerfs(tableQueryPod, inducerAndInduced);
			}
		}

		transferSizeAndCost(inducerAndInduced, executionFeedfack);
		return stepToSuppressedValues;
	}

	protected Set<String> getSuppressedGroupBy(IGroupBy generated, IGroupBy suppressed) {
		Set<String> queriedColumns = generated.getNameToColumn().keySet();
		Set<String> withoutSuppressedColumns = suppressed.getNameToColumn().keySet();
		Set<String> suppressedView = Sets.difference(queriedColumns, withoutSuppressedColumns);
		return ImmutableSet.copyOf(suppressedView);
	}

	protected void transferSizeAndCost(SplitTableQueries inducerAndInduced, ISinkExecutionFeedback executionFeedfack) {
		inducerAndInduced.getStepToCost()
				.entrySet()
				.stream()
				.forEach(e -> executionFeedfack.registerExecutionFeedback(e.getKey(), e.getValue()));
	}

	protected DagExplainer makeDagExplainer() {
		return DagExplainer.builder().eventBus(eventBus).build();
	}

	protected void explainDagSteps(QueryPod tableQueryPod, IHasDagFromInducedToInducer queryStepsDag) {
		makeDagExplainer().explain(tableQueryPod.getQueryId(), queryStepsDag);
	}

	protected DagExplainer makeDagExplainerForPerfs() {
		return DagExplainerForPerfs.builder().eventBus(eventBus).build();
	}

	protected void explainDagPerfs(QueryPod tableQueryPod, IHasDagFromInducedToInducer queryStepsDag) {
		makeDagExplainerForPerfs().explain(tableQueryPod.getQueryId(), queryStepsDag);
	}

	// Manages concurrency: the logic here should be strictly minimal on-top of concurrency
	protected Map<CubeQueryStep, ICuboid> executeTableQueries(ISinkExecutionFeedback sinkExecutionFeedback,
			IHasTableQueryForSteps tableQueries) {
		try {
			return queryPod.getExecutorService().submit(() -> {
				Stream<TableQueryV3> tableQueriesStream = tableQueries.getTableQueries().stream();

				if (StandardQueryOptions.CONCURRENT.isActive(queryPod.getOptions())) {
					tableQueriesStream = tableQueriesStream.parallel();
				}
				Map<CubeQueryStep, ICuboid> queryStepToValuesInner = new ConcurrentHashMap<>();
				tableQueriesStream.forEach(tableQuery -> {
					eventBus.post(TableStepIsEvaluating.builder().tableQuery(tableQuery).source(this).build());

					IStopwatch stopWatch = factories.getStopwatchFactory().createStarted();

					Map<CubeQueryStep, ICuboid> queryStepToValues =
							processOneTableQuery(sinkExecutionFeedback, tableQueries, tableQuery);

					Duration elapsed = stopWatch.elapsed();
					eventBus.post(TableStepIsCompleted.builder()
							.tableQuery(tableQuery)
							.nbCells(queryStepToValues.values().stream().mapToLong(ICuboid::size).sum())
							.source(this)
							.duration(elapsed)
							.build());

					eventBus.post(AdhocLogEvent.builder()
							.debug(queryPod.isDebug())
							.explain(queryPod.isExplain())
							.performance(true)
							.message(formatPerfLog("\\------ time=%s for tableQuery on %s", elapsed, tableQuery))
							.source(this)
							.build());

					// BEWARE Could we have multiple TableQueryV2 computing the same CubeQueryStep?
					queryStepToValuesInner.putAll(queryStepToValues);
				});

				return queryStepToValuesInner;
			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted", e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof IllegalStateException) {
				throw new IllegalStateException("Failed query on table=%s".formatted(queryPod.getTable().getName()), e);
			} else {
				throw new IllegalArgumentException("Failed query on table=%s".formatted(queryPod.getTable().getName()),
						e);
			}
		}
	}

	protected String formatPerfLog(String template, Duration elapsed, TableQueryV3 tableQuery) {
		return template.formatted(PepperLogHelper.humanDuration(elapsed.toMillis()), toPerfLog(tableQuery));
	}

	protected Map<CubeQueryStep, ICuboid> processOneTableQuery(ISinkExecutionFeedback sinkExecutionFeedback,
			IHasTableQueryForSteps tableQueries,
			TableQueryV3 tableQuery) {

		IStopwatch stopWatchSinking;

		Map<CubeQueryStep, ICuboid> stepToValues;

		IStopwatch openingStopwatch = factories.getStopwatchFactory().createStarted();
		// Open the stream: the table may or may not return after the actual execution
		try (ITabularRecordStream rowsStream = openTableStream(tableQuery)) {
			if (queryPod.isDebugOrExplain()) {
				// JooQ may be slow to load some classes
				// Slowness also due to fetching stream characteristics, which actually open the query
				Duration openingElasped = openingStopwatch.elapsed();
				eventBus.post(AdhocLogEvent.builder()
						.debug(queryPod.isDebug())
						.explain(queryPod.isExplain())
						.performance(true)
						.message(formatPerfLog("/-- time=%s for openingStream", openingElasped, tableQuery))
						.source(this)
						.build());
			}

			stopWatchSinking = factories.getStopwatchFactory().createStarted();
			stepToValues = aggregateStreamToAggregates(tableQueries, tableQuery, rowsStream);
		}

		Duration elapsed = stopWatchSinking.elapsed();
		reportOnTableQuery(tableQuery, sinkExecutionFeedback, elapsed, stepToValues);

		return stepToValues;
	}

	// `InsufficientStringBufferDeclaration`: Unclear if we prefer a MagicNumber
	@SuppressWarnings("PMD.InsufficientStringBufferDeclaration")
	protected String toPerfLog(TableQueryV3 tableQuery) {
		String measures = tableQuery.getAggregators().stream().map(this::toPerfLog).collect(Collectors.joining(", "));

		StringBuilder sb = new StringBuilder();

		sb.append("SELECT ").append(measures);

		if (!ISliceFilter.MATCH_ALL.equals(tableQuery.getFilter())) {
			sb.append(" WHERE ").append(tableQuery.getFilter());
		}

		IGroupBy groupBy;
		if (tableQuery.getGroupBys().isEmpty()) {
			sb.append(" GROUP BY ()");
		} else if (tableQuery.getGroupBys().size() == 1) {
			groupBy = AdhocCollectionHelpers.getFirst(tableQuery.getGroupBys());

			sb.append(" GROUP BY ").append(groupBy);
		} else {
			String groupByClause = tableQuery.getGroupBys()
					.stream()
					.map(IGroupBy::toString)
					.collect(Collectors.joining(",", "(", ")"));
			sb.append(" GROUPING SETS ").append(groupByClause);
		}

		return sb.toString();
	}

	protected String toPerfLog(CubeQueryStep cubeQueryStep) {
		TableQueryV3 tableQueries = TableQueryV3.edit(TableQuery.edit(cubeQueryStep).build()).build();
		return toPerfLog(tableQueries);
	}

	protected String toPerfLog(FilteredAggregator fa) {
		if (fa.getFilter().isMatchAll()) {
			return fa.getAggregator().toString();
		} else {
			return fa.getAggregator() + " FILTER(" + fa.getFilter() + ")";
		}
	}

	protected void reportOnTableQuery(TableQueryV3 tableQuery,
			ISinkExecutionFeedback sinkExecutionFeedback,
			Duration elapsed,
			Map<CubeQueryStep, ICuboid> oneQueryStepToValues) {
		boolean isExplain = queryPod.isDebugOrExplain();

		if (isExplain) {
			eventBus.post(AdhocLogEvent.builder()
					.debug(queryPod.isDebug())
					.explain(queryPod.isExplain())
					.message("/-- %s inducers from %s".formatted(oneQueryStepToValues.size(), toPerfLog(tableQuery)))
					.source(this)
					.build());
		}

		int lastStepIndex = oneQueryStepToValues.size() - 1;
		AtomicInteger queryStepIndex = new AtomicInteger();

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

			if (isExplain) {
				boolean isLast = queryStepIndex.getAndIncrement() == lastStepIndex;

				String template;
				if (isLast) {
					template = "\\-- step %s";
				} else {
					template = "|\\- step %s";
				}
				eventBus.post(AdhocLogEvent.builder()
						.debug(queryPod.isDebug())
						.explain(queryPod.isExplain())
						.message(template.formatted(toPerfLog(queryStep)))
						.source(this)
						.build());
			}
		});
	}

	/**
	 * 
	 * @param queryStepsDag
	 * @return a Stream over {@link CubeQueryStep} with resolved measures, guaranteed to be an {@link Aggregator}.
	 */
	protected Stream<CubeQueryStep> streamMissingRoots(QueryStepsDag queryStepsDag) {
		// BEWARE We could filter for `Aggregator` but it may be relevant to behave specifically on
		// `Transformator` with no undelryingSteps
		return queryStepsDag.getInducers()
				.stream()
				// Skip steps with a value in cache
				.filter(step -> !queryStepsDag.getStepToValues().containsKey(step))
				// Resolve aliases
				.map(step -> {
					IMeasure measure = queryPod.resolveIfRef(step.getMeasure());

					return CubeQueryStep.edit(step).measure(measure).build();
				})
				.filter(step -> {
					IMeasure measure = step.getMeasure();

					if (measure instanceof Aggregator) {
						return true;
					} else if (measure instanceof EmptyMeasure) {
						log.trace("An EmptyMeasure has no underlying measures");
						return false;
					} else {
						// Happens on Transformator with no underlying queryStep (no underlying measure, or planned as
						// having no underlying step (e.g. Filtrator with matchNone filter))
						log.debug("step={} has been planned having no underlying step", step);
						return false;
					}
				});
	}

	/**
	 * 
	 * @param queryStepsDag
	 * @return the Set of {@link TableQuery} to be executed.
	 */
	protected Set<CubeQueryStep> prepareForTable(QueryStepsDag queryStepsDag) {
		return streamMissingRoots(queryStepsDag).map(step -> {
			IMeasure measure = step.getMeasure();

			if (measure instanceof Aggregator) {
				return step;
			} else {
				throw new IllegalStateException("%s is not an Aggregator".formatted(step));
			}
		}).collect(ImmutableSet.toImmutableSet());
	}

	/**
	 * This step handles columns which are not relevant for the tables, but has not been suppressed by the measure tree.
	 * It typically happens when a column is introduced by some {@link Dispatchor} measure, but the actually requested
	 * measure is unrelated (which itself happens when selecting multiple measures, like `many2many+count(*)`).
	 * 
	 * 
	 * 
	 * @param generatedStep
	 * @return {@link TableQuery} where calculated columns has been suppressed.
	 */
	protected CubeQueryStep suppressGeneratedColumns(CubeQueryStep generatedStep) {
		// We list the generatedColumns instead of listing the table columns as many tables has lax resolution of
		// columns (e.g. given a joined `tableName.fieldName`, `fieldName` is a valid columnName. `.getColumns` would
		// probably return only one of the 2).
		Set<String> generatedColumns = queryPod.getColumnsManager()
				.getGeneratedColumns(factories.getOperatorFactory(),
						queryPod.getForest().getMeasures(),
						IValueMatcher.MATCH_ALL)
				.stream()
				.flatMap(cg -> cg.getColumnTypes().keySet().stream())
				.collect(ImmutableSet.toImmutableSet());

		Set<String> groupedByCubeColumns = generatedStep.getGroupBy().getGroupedByColumns();

		var edited = generatedStep.toBuilder();

		Set<String> generatedColumnToSuppressFromGroupBy = Sets.intersection(groupedByCubeColumns, generatedColumns);
		if (!generatedColumnToSuppressFromGroupBy.isEmpty()) {
			// All columns has been validated as being generated
			IGroupBy originalGroupby = generatedStep.getGroupBy();
			IGroupBy suppressedGroupby =
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
				ImmutableSet.copyOf(FilterHelpers.getFilteredColumns(generatedStep.getFilter()));
		Set<String> generatedColumnToSuppressFromFilter =
				Sets.intersection(filteredCubeColumnsToSuppress, generatedColumns);

		if (!generatedColumnToSuppressFromFilter.isEmpty()) {
			ISliceFilter originalFilter = generatedStep.getFilter();
			ISliceFilter suppressedFilter = SimpleFilterEditor.suppressColumn(originalFilter,
					generatedColumnToSuppressFromFilter,
					Optional.of(filterOptimizerSupplier.get()));

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

	protected ITabularRecordStream openTableStream(TableQueryV3 tableQuery) {
		return queryPod.getColumnsManager().openTableStream(queryPod, tableQuery);
	}

	protected Map<CubeQueryStep, ICuboid> aggregateStreamToAggregates(IHasTableQueryForSteps tableQueries,
			TableQueryV3 query,
			ITabularRecordStream stream) {
		IMultitypeMergeableGrid<IAdhocSlice> sliceToAggregates = mergeTableAggregates(query, stream);

		return splitTableGridToColumns(tableQueries, query, sliceToAggregates);
	}

	protected Map<CubeQueryStep, ICuboid> splitTableGridToColumns(IHasTableQueryForSteps tableQueries,
			TableQueryV3 query,
			IMultitypeMergeableGrid<IAdhocSlice> sliceToAggregates) {
		IStopwatch singToAggregatedStarted = factories.getStopwatchFactory().createStarted();

		Map<CubeQueryStep, ICuboid> immutableChunks = toCuboids(tableQueries, query, sliceToAggregates);

		// BEWARE This timing is independent of the table
		Duration elapsed = singToAggregatedStarted.elapsed();
		if (queryPod.isDebugOrExplain()) {
			long[] sizes = immutableChunks.values().stream().mapToLong(ICuboid::size).toArray();

			if (queryPod.isDebug()) {
				long totalSize = LongStream.of(sizes).sum();

				eventBus.post(AdhocLogEvent.builder()
						.debug(true)
						.performance(true)
						.message("|/- time=%s sizes=%s total_size=%s for sortingColumns".formatted(PepperLogHelper
								.humanDuration(elapsed.toMillis()), Arrays.toString(sizes), totalSize))
						.source(this)
						.build());
			} else if (queryPod.isExplain()) {
				eventBus.post(AdhocLogEvent.builder()
						.explain(true)
						.performance(true)
						.message("|/- time=%s sizes=%s for sortingColumns"
								.formatted(PepperLogHelper.humanDuration(elapsed.toMillis()), Arrays.toString(sizes)))
						.source(this)
						.build());
			}
		}
		return immutableChunks;
	}

	@Deprecated
	protected IMultitypeMergeableGrid<IAdhocSlice> mergeTableAggregates(TableQueryV2 query,
			ITabularRecordStream stream) {
		return mergeTableAggregates(TableQueryV3.edit(query).build(), stream);
	}

	protected IMultitypeMergeableGrid<IAdhocSlice> mergeTableAggregates(TableQueryV3 query,
			ITabularRecordStream stream) {
		IStopwatch stopWatch = factories.getStopwatchFactory().createStarted();

		IMultitypeMergeableGrid<IAdhocSlice> sliceToAggregates = mergeTableAggregates2(query, stream);

		// BEWARE This timing is dependent of the table
		Duration elapsed = stopWatch.elapsed();
		if (queryPod.isDebug()) {
			long totalSize = query.getAggregators().stream().mapToLong(sliceToAggregates::size).sum();

			eventBus.post(AdhocLogEvent.builder()
					.debug(queryPod.isDebug())
					.performance(true)
					.message("|/- time=%s size=%s for mergingAggregates"
							.formatted(PepperLogHelper.humanDuration(elapsed.toMillis()), totalSize))
					.source(this)
					.build());
		} else if (queryPod.isExplain()) {
			eventBus.post(AdhocLogEvent.builder()
					.explain(queryPod.isExplain())
					.performance(true)
					.message("|/- time=%s for mergingAggregates"
							.formatted(PepperLogHelper.humanDuration(elapsed.toMillis())))
					.source(this)
					.build());
		}
		return sliceToAggregates;
	}

	protected IMultitypeMergeableGrid<IAdhocSlice> mergeTableAggregates2(TableQueryV3 tableQuery,
			ITabularRecordStream stream) {
		ITabularRecordStreamReducer streamReducer = makeTabularRecordStreamReducer(
				// (TableQueryV3) stream.getTableQuery()
				tableQuery);

		return streamReducer.reduce(stream);
	}

	protected ITabularRecordStreamReducer makeTabularRecordStreamReducer(TableQueryV3 tableQuery) {
		return TabularRecordStreamReducer.builder()
				.operatorFactory(factories.getOperatorFactory())
				.sliceFactory(queryPod.getSliceFactory())
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
	 * 
	 * @param tableQueries
	 * @param query
	 * @param coordinatesToAggregates
	 * @return a {@link Map} from each {@link Aggregator} to the column of values
	 */
	protected Map<CubeQueryStep, ICuboid> toCuboids(IHasTableQueryForSteps tableQueries,
			TableQueryV3 query,
			IMultitypeMergeableGrid<IAdhocSlice> coordinatesToAggregates) {
		Stream<StepAndFilteredAggregator> stepStream =
				tableQueries.forEachCubeQuerySteps(query, filterOptimizerSupplier.get());

		if (StandardQueryOptions.CONCURRENT.isActive(query.getOptions())) {
			// TODO Should we enforce being in the proper Executor/FJP?
			stepStream = stepStream.parallel();
		}

		return stepStream.map(r -> {
			FilteredAggregator filteredAggregator = r.aggregator();
			CubeQueryStep queryStep = r.step();

			// `.closeColumn` may be an expensive operation. e.g. it may sort slices.
			// TODO do close only if the queryStep is actually relevant for the rest of the DAG.
			IMultitypeColumnFastGet<IAdhocSlice> values =
					coordinatesToAggregates.closeColumn(queryStep, filteredAggregator);

			// The aggregation step is done: the storage is supposed not to be edited: we
			// re-use it in place, to spare a copy to an immutable container
			return Map.entry(queryStep, Cuboid.forGroupBy(queryStep).values(values).build());
		}).collect(PepperStreamHelperHacked.toLinkedMap(Map.Entry::getKey, Map.Entry::getValue));
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
	protected ICuboid restoreSuppressedGroupBy(CubeQueryStep queryStep, Set<String> suppressedColumns, ICuboid column) {
		Map<String, ?> constantValues = valuesForSuppressedColumns(suppressedColumns, queryStep);

		boolean match = FilterMatcher.builder()
				.sliceFactory(factories.getSliceFactory())
				.filter(queryStep.getFilter())
				.onMissingColumn(cf -> {
					// We test only suppressedColumns for now
					// TODO We should actually test each slice individually. It would lead to issues on complex filters
					// (e.g. with OR), and fails around `eu.solven.adhoc.engine.step.SliceAsMapWithStep.asFilter()`.
					return true;
				})
				.build()
				.match(constantValues);

		if (match) {
			return column.mask(constantValues);
		} else {
			log.debug("suppressedColumns={} are filtered out by {}", constantValues, queryStep.getFilter());
			return Cuboid.empty();
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
		return suppressedColumns.stream()
				.collect(PepperStreamHelperHacked.toLinkedMap(Function.identity(),
						c -> IColumnGenerator.COORDINATE_GENERATED));
	}

	/**
	 * 
	 * 
	 * @param stepToValues
	 *            a mutable {@link Map}. May need to be thread-safe.
	 * @param inducerAndInduced
	 */
	protected void walkUpInducedDag(Map<CubeQueryStep, ICuboid> stepToValues, SplitTableQueries inducerAndInduced) {
		Consumer<? super CubeQueryStep> queryStepConsumer = induced -> {
			try {
				evaluateInduced(stepToValues, inducerAndInduced, induced);
			} catch (RuntimeException e) {
				throw AdhocExceptionHelpers.wrap("Issue inducing step=%s".formatted(induced), e);
			}
		};

		QueryEngineConcurrencyHelper.walkUpDag(queryPod, inducerAndInduced, stepToValues, queryStepConsumer);
	}

	protected void evaluateInduced(Map<CubeQueryStep, ICuboid> stepToValues,
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
			eventBus.post(QueryStepIsEvaluating.builder().queryStep(induced).source(this).build());

			IStopwatch stopWatch = factories.getStopwatchFactory().createStarted();

			IMultitypeMergeableColumn<IAdhocSlice> inducedValues =
					inducer.evaluateInduced(queryPod, inducerAndInduced, stepToValues, induced);

			Duration elapsed = stopWatch.elapsed();
			eventBus.post(QueryStepIsCompleted.builder()
					.querystep(induced)
					.nbCells(inducedValues.size())
					.source(this)
					.duration(elapsed)
					.build());

			ICuboid alreadyPresent =
					stepToValues.putIfAbsent(induced, Cuboid.forGroupBy(induced).values(inducedValues).build());
			if (alreadyPresent != null) {
				// This may happen on CONCURRENT queries, as we might request the same underlying multiple times.
				log.warn("Already present: induced={} (from {} to {}). Should not happen since 0.0.14",
						induced,
						alreadyPresent.size(),
						inducedValues.size());
			}

			inducerAndInduced.registerExecutionFeedback(induced,
					SizeAndDuration.builder().size(inducedValues.size()).duration(elapsed).build());
		}
	}
}
