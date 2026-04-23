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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import eu.solven.adhoc.collection.AdhocCollectionHelpers;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ICalculatedColumn;
import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.filter.FilterMatcher;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.dataframe.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.ISinkExecutionFeedback;
import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.concurrent.QueryEngineConcurrencyHelper;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.observability.DagExplainer;
import eu.solven.adhoc.engine.observability.DagExplainerForPerfs;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
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
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByHelpers;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocBlackHole;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.core.PepperStreamHelper;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds the execution logic related with an {@link ITableQueryEngineFactory}, given a {@link ITableQueryFactory} in the
 * context of a single {@link TableQuery}.
 *
 * @author Benoit Lacelle
 * @see TableQueryEngineFactory
 */
@Slf4j
@Builder
@SuppressWarnings({ "PMD.GodClass", "PMD.CouplingBetweenObjects" })
// https://math.stackexchange.com/questions/2966359/how-to-calculate-cost-in-discrete-markov-transitions
public class TableQueryEngine implements ITableQueryEngine {

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
	final ITableQueryFactory tableQueryFactory;

	final ITableQueryInducer inducer;

	final Supplier<Set<String>> generatedColumnsSupplier = Suppliers.memoize(this::computeGeneratedColumns);

	final Supplier<IFilterOptimizer> filterOptimizerSupplier = Suppliers.memoize(() -> {
		if (getTableQueryFactory() instanceof IHasFilterOptimizer hasFilterOptimizer) {
			// Most ITableQueryOptimizer has a filterOptimizerWithCache
			return hasFilterOptimizer.getFilterOptimizer();
		} else {
			return this.getFactories().getFilterOptimizerFactory().makeOptimizerWithCache();
		}
	});

	@Override
	public Map<TableQueryStep, ICuboid> executeTableQueries(QueryStepsDag queryStepsDag) {
		ISinkExecutionFeedback executionFeedfack = prepareExecutionFeedback(queryStepsDag);

		// Collect the tableQueries given the TableQueryStep, essentially by focusing on aggregated measures
		Set<TableQueryStep> tableQuerySteps = prepareForTable(queryStepsDag);

		Map<TableQueryStep, TableQueryStep> calculatedToSuppressed = new LinkedHashMap<>();

		tableQuerySteps.forEach(generatedStep -> {
			calculatedToSuppressed.put(generatedStep, suppressGeneratedColumns(generatedStep));
		});

		Set<TableQueryStep> suppressedQuerySteps = ImmutableSet.copyOf(calculatedToSuppressed.values());
		log.debug("From {} generated to {} suppressed", calculatedToSuppressed.size(), suppressedQuerySteps.size());

		Map<TableQueryStep, ICuboid> stepToSuppressedValues =
				executeTableQueries(suppressedQuerySteps, executionFeedfack);

		return restoreSuppressedGroupBy(calculatedToSuppressed, stepToSuppressedValues);
	}

	private ISinkExecutionFeedback prepareExecutionFeedback(QueryStepsDag queryStepsDag) {
		// This is probably bad design
		// Needed to transfer the explicitNodes from tableSteps into the rootNodes in cubeSteps
		return (queryStep, sizeAndDuration) -> {
			if (queryStepsDag.getMultigraph().containsVertex(CubeQueryStep.edit(queryStep).build())) {
				queryStepsDag.registerExecutionFeedback(queryStep, sizeAndDuration);
			}
		};
	}

	protected Map<TableQueryStep, ICuboid> restoreSuppressedGroupBy(
			Map<TableQueryStep, TableQueryStep> calculatedToSuppressed,
			Map<TableQueryStep, ICuboid> stepToSuppressedValues) {
		SetMultimap<TableQueryStep, TableQueryStep> asMultimap = Multimaps.forMap(calculatedToSuppressed);
		SetMultimap<TableQueryStep, TableQueryStep> suppressedToGenerated =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();
		Multimaps.invertFrom(asMultimap, suppressedToGenerated);

		Map<TableQueryStep, ICuboid> stepToValues = new LinkedHashMap<>();
		stepToSuppressedValues.forEach((suppressedStep, suppressedValue) -> {
			Set<TableQueryStep> generated = suppressedToGenerated.get(suppressedStep);

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
		return stepToValues;
	}

	protected Map<TableQueryStep, ICuboid> executeTableQueries(Set<TableQueryStep> steps,
			ISinkExecutionFeedback executionFeedfack) {
		// Split these queries given inducing logic. (e.g. `SUM(a) GROUP BY b` may be induced by `SUM(a) GROUP BY b, c`)
		SplitTableQueries withoutShared = tableQueryFactory.splitInduced(queryPod, steps);

		// Evaluate shared nodes asynchronously, in parallel of tableQueries
		ListenableFuture<IAdhocDag<TableQueryStep>> futureSharedGraph = queryPod.getExecutorService().submit(() -> {
			return withoutShared.getLazyGraph().apply(queryPod.getExecutorService());
		});

		// Execute the actual tableQueries
		Map<TableQueryStep, ICuboid> stepToValuesFromtableWrapper = executeTableQueries(withoutShared, withoutShared);

		// Wait for sharedNodes execution
		SplitTableQueries withShared = waitAndMergeSharedNodes(withoutShared, futureSharedGraph);

		QueryPod tableQueryPod = queryPod.asTableQuery();

		// Switch to a ConcurrentMap as `walkUpInducedDag` may be concurrent
		ConcurrentMap<TableQueryStep, ICuboid> stepToValues = new ConcurrentHashMap<>(stepToValuesFromtableWrapper);
		{
			if (queryPod.isDebugOrExplain()) {
				explainDagSteps(tableQueryPod, withShared);
			}

			// Evaluated the induced tableQueries
			// BEWARE This will also register some shared nodes, which are irrelevant to the output but useful for the
			// DAG of size-cost
			walkUpInducedDag(stepToValues, withShared);

			if (queryPod.isDebugOrExplain()) {
				explainDagPerfs(tableQueryPod, withShared);
			}
		}

		transferSizeAndCost(withShared, executionFeedfack);
		return stepToValues;
	}

	public static SplitTableQueries waitAndMergeSharedNodes(SplitTableQueries withoutShared,
			ListenableFuture<IAdhocDag<TableQueryStep>> futureSharedGraph) {
		IAdhocDag<TableQueryStep> sharedGraph = Futures.getUnchecked(futureSharedGraph);

		// No need to merge as getLazyGraph includes the original graph
		// IAdhocDag<TableQueryStep> withSharedGraph = GraphHelpers.copy(withoutShared.getInducedToInducer());
		// Graphs.addGraph(withSharedGraph, sharedGraph);

		return withoutShared.toBuilder().inducedToInducer(sharedGraph).build();
	}

	protected Set<String> getSuppressedGroupBy(IGroupBy generated, IGroupBy suppressed) {
		Set<String> queriedColumns = generated.getSortedNameToColumn().keySet();
		Set<String> withoutSuppressedColumns = suppressed.getSortedNameToColumn().keySet();
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

	protected void explainDagSteps(QueryPod tableQueryPod, IHasDagFromInducedToInducer<?> queryStepsDag) {
		makeDagExplainer().explain(tableQueryPod.getQueryId(), queryStepsDag);
	}

	protected DagExplainer makeDagExplainerForPerfs() {
		return DagExplainerForPerfs.builder().eventBus(eventBus).build();
	}

	protected void explainDagPerfs(QueryPod tableQueryPod, IHasDagFromInducedToInducer<?> queryStepsDag) {
		makeDagExplainerForPerfs().explain(tableQueryPod.getQueryId(), queryStepsDag);
	}

	// Manages concurrency: the logic here should be strictly minimal on-top of concurrency
	@SuppressWarnings("PMD.CloseResource")
	protected Map<TableQueryStep, ICuboid> executeTableQueries(ISinkExecutionFeedback sinkExecutionFeedback,
			IHasTableQueryForSteps hasTableQueries) {
		try {
			Set<TableQueryV4> tableQueries = hasTableQueries.getTableQueries();

			List<Map<TableQueryStep, ICuboid>> listStepsToCuboids;
			// With Virtual Threads, all blocking I/O is acceptable on the executor; no separate IO pool is needed.
			ListeningExecutorService executorService = queryPod.getExecutorService();
			if (StandardQueryOptions.CONCURRENT.isActive(queryPod.getOptions())) {
				List<CompletableFuture<Map<TableQueryStep, ICuboid>>> futures = tableQueries.stream()
						.map(tableQuery -> CompletableFuture.supplyAsync(
								() -> processOneTableQuery(sinkExecutionFeedback, hasTableQueries, tableQuery),
								executorService))
						.toList();
				// join() on each future blocks until it completes (or propagates a failure)
				listStepsToCuboids =
						futures.stream().map(CompletableFuture::join).collect(ImmutableList.toImmutableList());
			} else {
				// Sequential path: run every table query sequentially on the executor
				var completable = CompletableFuture.supplyAsync(() -> tableQueries.stream()
						.map(tableQuery -> processOneTableQuery(sinkExecutionFeedback, hasTableQueries, tableQuery))
						.collect(ImmutableList.toImmutableList()), executorService);
				listStepsToCuboids = completable.join();
			}

			Map<TableQueryStep, ICuboid> stepsToCuboid = new LinkedHashMap<>();

			// BEWARE Could we have multiple TableQueryV4 computing the same TableQueryStep?
			listStepsToCuboids.forEach(stepsToCuboid::putAll);

			return stepsToCuboid;
		} catch (RuntimeException e) {
			throw AdhocExceptionHelpers.wrap("Failed query on table=%s".formatted(queryPod.getTable().getName()), e);
		}
	}

	protected String formatPerfLog(String template, Duration elapsed, TableQueryV4 tableQuery) {
		return template.formatted(PepperLogHelper.humanDuration(elapsed.toMillis()), toPerfLog(tableQuery));
	}

	protected Map<TableQueryStep, ICuboid> processOneTableQuery(ISinkExecutionFeedback sinkExecutionFeedback,
			IHasTableQueryForSteps tableQueries,
			TableQueryV4 tableQuery) {
		eventBus.post(TableStepIsEvaluating.builder().tableQuery(tableQuery).source(this).build());

		IStopwatch stopWatch = factories.getStopwatchFactory().createStarted();

		Map<TableQueryStep, ICuboid> stepToValues =
				processOneTableQueryV4(sinkExecutionFeedback, tableQueries, tableQuery);

		Duration elapsed = stopWatch.elapsed();
		eventBus.post(TableStepIsCompleted.builder()
				.tableQuery(tableQuery)
				.nbCells(stepToValues.values().stream().mapToLong(ICuboid::size).sum())
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

		return stepToValues;
	}

	@SuppressWarnings("PMD.CloseResource")
	protected Map<TableQueryStep, ICuboid> processOneTableQueryV4(ISinkExecutionFeedback sinkExecutionFeedback,
			IHasTableQueryForSteps tableQueries,
			TableQueryV4 tableQuery) {
		List<TableQueryV4> nonAmbiguousQueries = splitForNonAmbiguousColumns(tableQuery);

		List<Map<TableQueryStep, ICuboid>> eachStepToValues;

		if (StandardQueryOptions.CONCURRENT.isActive(queryPod.getOptions())) {
			ListeningExecutorService service = queryPod.getExecutorService();

			List<ListenableFuture<Map<TableQueryStep, ICuboid>>> futuresEachStepToValues = nonAmbiguousQueries.stream()
					.map(nonAmbiguousQuery -> service.submit(
							() -> executeOneNonAmbiguous(sinkExecutionFeedback, tableQueries, nonAmbiguousQuery)))
					.toList();

			eachStepToValues = Futures.getUnchecked(Futures.allAsList(futuresEachStepToValues));
		} else {
			eachStepToValues = nonAmbiguousQueries.stream()
					.map(nonAmbiguousQuery -> executeOneNonAmbiguous(sinkExecutionFeedback,
							tableQueries,
							nonAmbiguousQuery))
					.toList();
		}

		Map<TableQueryStep, ICuboid> allStepToValues = new LinkedHashMap<>();
		eachStepToValues.forEach(allStepToValues::putAll);
		return allStepToValues;
	}

	protected Map<TableQueryStep, ICuboid> executeOneNonAmbiguous(ISinkExecutionFeedback sinkExecutionFeedback,
			IHasTableQueryForSteps tableQueries,
			TableQueryV4 tableQuery) {
		IStopwatch stopWatchSinking;
		Map<TableQueryStep, ICuboid> stepToValues;

		IStopwatch openingStopwatch = factories.getStopwatchFactory().createStarted();
		// Open the stream: the table may or may not return after the actual execution
		try (ITabularRecordStream rowsStream = openTableStream(tableQuery)) {
			if (queryPod.isDebugOrExplain()) {
				// JooQ may be slow to load some classes
				// Slowness also due to fetching stream characteristics, which actually open the
				// query
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

	/**
	 * Relevant for ICalculatedColumn, as we must different between a referencedColumn and a calculated column even if
	 * they have the same name.
	 * 
	 * @param tableQuery
	 * @return
	 */
	protected List<TableQueryV4> splitForNonAmbiguousColumns(TableQueryV4 tableQuery) {
		SetMultimap<String, IAdhocColumn> nameToColumns =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();
		tableQuery.getGroupBys().forEach(gb -> {
			nameToColumns.putAll(Multimaps.forMap(gb.getSortedNameToColumn()));
		});

		if (nameToColumns.asMap().values().stream().allMatch(c -> c.size() == 1)) {
			// Not a single conflicting column
			return ImmutableList.of(tableQuery);
		}

		// TODO We have to reason to believe we may end in this case in, sometimes, not legitimate case
		// Typically, if `c` is computed, the actual groupBy should not have `c` in groupBy while it seems to persist
		List<TableQueryV4> split = new ArrayList<>();

		SetMultimap<IGroupBy, FilteredAggregator> leftToAdd =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();
		leftToAdd.putAll(tableQuery.getGroupByToAggregators());

		while (!leftToAdd.isEmpty()) {
			SetMultimap<IGroupBy, FilteredAggregator> nextQuery =
					MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

			leftToAdd.forEach((groupBy, aggregators) -> {
				if (noAmbiguity(nextQuery.keySet(), groupBy)) {
					nextQuery.put(groupBy, aggregators);
				}
			});

			if (nextQuery.isEmpty()) {
				throw new IllegalStateException("Should not be empty given %s".formatted(leftToAdd));
			}

			leftToAdd.keySet().removeAll(nextQuery.keySet());
			split.add(tableQuery.toBuilder().clearGroupByToAggregators().groupByToAggregators(nextQuery).build());
		}

		if (tableQuery.isDebugOrExplain()) {
			log.info("[EXPLAIN] Ambiguities in columnNames led to split into {} queries: (original={} split={})",
					split.size(),
					tableQuery,
					split);
		}

		return split;
	}

	protected boolean noAmbiguity(Set<IGroupBy> accepted, IGroupBy candidate) {
		SetMultimap<String, IAdhocColumn> nameToColumns =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();
		accepted.forEach(gb -> {
			nameToColumns.putAll(Multimaps.forMap(gb.getSortedNameToColumn()));
		});
		nameToColumns.putAll(Multimaps.forMap(candidate.getSortedNameToColumn()));

		return nameToColumns.asMap().values().stream().allMatch(c -> c.size() == 1);
	}

	// `InsufficientStringBufferDeclaration`: Unclear if we prefer a MagicNumber
	protected String toPerfLog(TableQueryV4 tableQuery) {
		// isPerfectV3() is the shared flag: JooqTableQueryFactory currently ignores it and always uses
		// asCoveringV3() (GROUPING SETS), but the log reflects what the strategy should ideally be.
		if (tableQuery.isPerfectV3()) {
			return toPerfLog(tableQuery.asCoveringV3());
		}

		return tableQuery.streamV3().map(this::toPerfLog).collect(Collectors.joining(" UNION ALL "));
	}

	@SuppressWarnings("PMD.InsufficientStringBufferDeclaration")
	protected String toPerfLog(TableQueryV3 tableQuery) {
		String measures = tableQuery.getAggregators().stream().map(this::toPerfLog).collect(Collectors.joining(", "));

		StringBuilder sb = new StringBuilder();

		sb.append("SELECT ").append(measures);

		if (!ISliceFilter.MATCH_ALL.equals(tableQuery.getFilter())) {
			sb.append(" WHERE ").append(tableQuery.getFilter());
		}

		if (tableQuery.getGroupBys().isEmpty()) {
			sb.append(" GROUP BY ()");
		} else if (tableQuery.getGroupBys().size() == 1) {
			sb.append(" GROUP BY ").append(AdhocCollectionHelpers.getFirst(tableQuery.getGroupBys()));
		} else {
			String groupByClause = tableQuery.getGroupBys()
					.stream()
					.map(IGroupBy::toString)
					.collect(Collectors.joining(",", "(", ")"));
			sb.append(" GROUPING SETS ").append(groupByClause);
		}

		return sb.toString();
	}

	protected String toPerfLog(TableQueryStep step) {
		TableQueryV4 tableQueries = TableQueryV4.edit(TableQuery.edit(step).build()).build();
		return toPerfLog(tableQueries);
	}

	protected String toPerfLog(FilteredAggregator fa) {
		if (fa.getFilter().isMatchAll()) {
			return fa.getAggregator().toString();
		} else {
			return fa.getAggregator() + " FILTER(" + fa.getFilter() + ")";
		}
	}

	protected void reportOnTableQuery(TableQueryV4 tableQuery,
			ISinkExecutionFeedback sinkExecutionFeedback,
			Duration elapsed,
			Map<TableQueryStep, ICuboid> oneQueryStepToValues) {
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
	 * @return a Stream over {@link TableQueryStep} with resolved measures, guaranteed to be an {@link Aggregator}.
	 */
	protected Stream<TableQueryStep> streamMissingRoots(QueryStepsDag queryStepsDag) {
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
				})
				.map(cubeStep -> TableQueryStep.edit(cubeStep).build());
	}

	/**
	 * 
	 * @param queryStepsDag
	 * @return the Set of {@link TableQuery} to be executed.
	 */
	protected Set<TableQueryStep> prepareForTable(QueryStepsDag queryStepsDag) {
		return streamMissingRoots(queryStepsDag).map(step -> {
			IMeasure measure = step.getMeasure();

			if (measure instanceof Aggregator) {
				return TableQueryStep.edit(step).build();
			} else {
				throw new IllegalStateException("%s is not an Aggregator".formatted(step));
			}
		}).collect(ImmutableSet.toImmutableSet());
	}

	/**
	 * @return the set of column names that are generated (e.g. by {@link IColumnGenerator} measures), constant for the
	 *         lifetime of this bootstrapped execution.
	 */
	protected Set<String> computeGeneratedColumns() {
		// We list the generatedColumns instead of listing the table columns as many tables has lax resolution of
		// columns (e.g. given a joined `tableName.fieldName`, `fieldName` is a valid columnName. `.getColumns` would
		// probably return only one of the 2).
		return queryPod.getColumnsManager()
				.getGeneratedColumns(factories.getOperatorFactory(),
						queryPod.getForest().getMeasures(),
						IValueMatcher.MATCH_ALL)
				.stream()
				.flatMap(cg -> cg.getColumnTypes().keySet().stream())
				.collect(ImmutableSet.toImmutableSet());
	}

	/**
	 * This step handles columns which are not relevant for the tables, but has not been suppressed by the measure tree.
	 * It typically happens when a column is introduced by some {@link Dispatchor} measure, but the actually requested
	 * measure is unrelated (which itself happens when selecting multiple measures, like `many2many+count(*)`).
	 *
	 * @param generatedStep
	 * @return {@link TableQuery} where calculated columns has been suppressed.
	 */
	protected TableQueryStep suppressGeneratedColumns(TableQueryStep generatedStep) {
		Set<String> generatedColumns = generatedColumnsSupplier.get();

		IGroupBy originalGroupby = generatedStep.getGroupBy();

		var edited = generatedStep.toBuilder();

		Set<String> generatedColumnToSuppressFromGroupBy =
				generatedColumnsToSuppressFromGroupBy(originalGroupby, generatedColumns);
		if (!generatedColumnToSuppressFromGroupBy.isEmpty()) {
			// All columns has been validated as being generated
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

	/**
	 * Compute the subset of groupBy columns that qualify for suppression, given the set of column names known to have
	 * been generated by a {@link IColumnGenerator}.
	 *
	 * A column qualifies only when its name matches a generated column AND its concrete {@link IAdhocColumn} is not an
	 * {@link ICalculatedColumn}. An {@link ICalculatedColumn} carries its own value-computation logic (e.g. a constant
	 * `*` coordinate for a grandTotal); collapsing it to the {@link IColumnGenerator} fallback
	 * (`COORDINATE_GENERATED="generated"`) would silently overwrite that value — and, when the same query also groups
	 * by the same column as a plain {@code ReferencedColumn}, produce two steps that emit the same slice key and crash
	 * at merge time. See {@code TestTableQuery_DuckDb_VaR#testGroupByScenarioIndex_withStar_countAsterisk}.
	 *
	 * @param originalGroupby
	 *            the {@link IGroupBy} of the current {@link TableQueryStep}.
	 * @param generatedColumns
	 *            names of columns generated by some {@link IColumnGenerator}.
	 * @return the names of columns which must be removed from the groupBy before calling the table.
	 */
	protected static Set<String> generatedColumnsToSuppressFromGroupBy(IGroupBy originalGroupby,
			Set<String> generatedColumns) {
		Set<String> groupedByCubeColumns = originalGroupby.getSortedColumns();
		return Sets.intersection(groupedByCubeColumns, generatedColumns)
				.stream()
				.filter(name -> !(originalGroupby.getSortedNameToColumn().get(name) instanceof ICalculatedColumn))
				.collect(ImmutableSet.toImmutableSet());
	}

	protected ITabularRecordStream openTableStream(TableQueryV4 tableQuery) {
		return queryPod.getColumnsManager().openTableStream(queryPod, tableQuery);
	}

	protected Map<TableQueryStep, ICuboid> aggregateStreamToAggregates(IHasTableQueryForSteps tableQueries,
			TableQueryV4 query,
			ITabularRecordStream stream) {
		IMultitypeMergeableGrid<ISlice> sliceToAggregates = mergeTableAggregates(query, stream);

		return splitTableGridToCuboids(tableQueries, query, sliceToAggregates);
	}

	protected Map<TableQueryStep, ICuboid> splitTableGridToCuboids(IHasTableQueryForSteps tableQueries,
			TableQueryV4 query,
			IMultitypeMergeableGrid<ISlice> sliceToAggregates) {
		IStopwatch singToAggregatedStarted = factories.getStopwatchFactory().createStarted();

		Map<TableQueryStep, ICuboid> immutableChunks = toCuboids(tableQueries, query, sliceToAggregates);

		// BEWARE This timing is independent of the table
		Duration elapsed = singToAggregatedStarted.elapsed();
		if (queryPod.isDebugOrExplain()) {
			long[] sizes = immutableChunks.values().stream().mapToLong(ICuboid::size).toArray();

			if (queryPod.isDebug()) {
				long totalSize = Arrays.stream(sizes).sum();

				eventBus.post(AdhocLogEvent.builder()
						.debug(true)
						.performance(true)
						.message("|/- time=%s sizes=%s total_size=%s for toCuboids".formatted(PepperLogHelper
								.humanDuration(elapsed.toMillis()), Arrays.toString(sizes), totalSize))
						.source(this)
						.build());
			} else if (queryPod.isExplain()) {
				eventBus.post(AdhocLogEvent.builder()
						.explain(true)
						.performance(true)
						.message("|/- time=%s sizes=%s for toCuboids"
								.formatted(PepperLogHelper.humanDuration(elapsed.toMillis()), Arrays.toString(sizes)))
						.source(this)
						.build());
			}
		}
		return immutableChunks;
	}

	@Deprecated
	protected IMultitypeMergeableGrid<ISlice> mergeTableAggregates(TableQueryV2 query, ITabularRecordStream stream) {
		return mergeTableAggregates(TableQueryV3.edit(query).build(), stream);
	}

	@Deprecated
	protected IMultitypeMergeableGrid<ISlice> mergeTableAggregates(TableQueryV3 query, ITabularRecordStream stream) {
		return mergeTableAggregates(TableQueryV4.edit(query).build(), stream);
	}

	protected IMultitypeMergeableGrid<ISlice> mergeTableAggregates(TableQueryV4 query, ITabularRecordStream stream) {
		IStopwatch stopWatch = factories.getStopwatchFactory().createStarted();

		IMultitypeMergeableGrid<ISlice> sliceToAggregates = mergeTableAggregates2(query, stream);

		// BEWARE This timing is dependent of the table
		Duration elapsed = stopWatch.elapsed();
		if (queryPod.isDebug()) {
			long totalSize = sliceToAggregates.getAggregators().stream().mapToLong(sliceToAggregates::size).sum();

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

	protected IMultitypeMergeableGrid<ISlice> mergeTableAggregates2(TableQueryV4 tableQuery,
			ITabularRecordStream stream) {
		ITabularRecordStreamReducer streamReducer = makeTabularRecordStreamReducer(tableQuery);

		return streamReducer.reduce(stream);
	}

	protected ITabularRecordStreamReducer makeTabularRecordStreamReducer(TableQueryV4 tableQuery) {
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
	protected Map<TableQueryStep, ICuboid> toCuboids(IHasTableQueryForSteps tableQueries,
			TableQueryV4 query,
			IMultitypeMergeableGrid<ISlice> coordinatesToAggregates) {
		Stream<StepAndFilteredAggregator> stepStream =
				tableQueries.forEachCubeQuerySteps(query, filterOptimizerSupplier.get());

		if (StandardQueryOptions.CONCURRENT.isActive(query.getOptions())) {
			// TODO Should we enforce being in the proper Executor/FJP?
			stepStream = stepStream.parallel();
		}

		return stepStream.map(r -> {
			FilteredAggregator filteredAggregator = r.aggregator();
			TableQueryStep queryStep = r.step();

			// `.closeColumn` may be an expensive operation. e.g. it may sort slices.
			// TODO do close only if the queryStep is actually relevant for the rest of the DAG.
			IMultitypeColumnFastGet<ISlice> values = coordinatesToAggregates.closeColumn(queryStep, filteredAggregator);

			// The aggregation step is done: the storage is supposed not to be edited: we
			// re-use it in place, to spare a copy to an immutable container
			return Map.entry(queryStep, Cuboid.forGroupBy(queryStep).values(values).build());
		}).collect(PepperStreamHelper.toLinkedMap(Map.Entry::getKey, Map.Entry::getValue));
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
	protected ICuboid restoreSuppressedGroupBy(TableQueryStep queryStep,
			Set<String> suppressedColumns,
			ICuboid column) {
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
	protected Map<String, ?> valuesForSuppressedColumns(Set<String> suppressedColumns, TableQueryStep queryStep) {
		return suppressedColumns.stream()
				.collect(PepperStreamHelper.toLinkedMap(Function.identity(),
						_ -> IColumnGenerator.COORDINATE_GENERATED));
	}

	/**
	 * 
	 * 
	 * @param stepToValues
	 *            a mutable {@link ConcurrentMap}.
	 * @param inducerAndInduced
	 */
	protected void walkUpInducedDag(ConcurrentMap<TableQueryStep, ICuboid> stepToValues,
			SplitTableQueries inducerAndInduced) {
		// TODO How to force the computation to the right executorService?
		// ITableQueryInducer depends on IInducedEvaluator, which may be a pure JVM-cpu implementation, or typically a
		// DuckDB-io-semaphore implementation.
		QueryEngineConcurrencyHelper.walkUpDag(queryPod, inducerAndInduced, stepToValues, induced -> {
			try {
				evaluateInduced(stepToValues, inducerAndInduced, induced);
			} catch (RuntimeException e) {
				throw AdhocExceptionHelpers.wrap("Issue inducing step=%s".formatted(induced), e);
			}
		});
	}

	protected void evaluateInduced(Map<TableQueryStep, ICuboid> stepToValues,
			SplitTableQueries inducerAndInduced,
			TableQueryStep induced) {
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

			IMultitypeMergeableColumn<ISlice> inducedValues =
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
