/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.engine;

import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.jgrapht.alg.interfaces.ShortestPathAlgorithm;
import org.jgrapht.alg.shortestpath.JohnsonShortestPaths;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.ListMapEntryBasedTabularViewDrillThrough;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.engine.concurrent.QueryEngineConcurrencyHelper;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.dag.AdhocDag;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.observability.AdhocQueryMonitor;
import eu.solven.adhoc.engine.observability.DagExplainer;
import eu.solven.adhoc.engine.observability.DagExplainerForPerfs;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.ITableQueryEngineFactory;
import eu.solven.adhoc.engine.tabular.TableQueryEngineFactory;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.eventbus.UnsafeAdhocEventBusHelpers;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.StringMatcher;
import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.operator.IHasOperatorFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.transformator.IHasAggregationKey;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.IMeasureQueryStep;
import eu.solven.adhoc.options.HasOptionsAndExecutorService;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.AdhocBlackHole;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * The default query-engine.
 *
 * @author Benoit Lacelle
 */
@Builder(toBuilder = true)
@Slf4j
@SuppressWarnings({ "PMD.GodClass" })
public class CubeQueryEngine implements ICubeQueryEngine, IHasOperatorFactory {
	private final UUID engineId = UUID.randomUUID();

	// This shall not conflict with any user measure
	private final String emptyMeasureName = "$ADHOC$empty-" + engineId;

	@NonNull
	@Default
	// @Getter is useful for tests. May be useful to help providing a relevant EventBus to other components.
	@Getter
	@SuppressWarnings("PMD.UnusedAssignment")
	final IAdhocFactories factories = AdhocFactoriesUnsafe.factories;

	@NonNull
	@Default
	// @Getter is useful for tests. May be useful to help providing a relevant EventBus to other components.
	@Getter
	@SuppressWarnings("PMD.UnusedAssignment")
	final IAdhocEventBus eventBus = UnsafeAdhocEventBusHelpers.safeWrapper(AdhocBlackHole.getInstance());

	@NonNull
	@VisibleForTesting
	@Getter
	ITableQueryEngineFactory tableQueryEngine;

	protected CubeQueryEngine(IAdhocFactories factories,
			IAdhocEventBus eventBus,
			ITableQueryEngineFactory tableQueryEngine) {
		if (tableQueryEngine == null) {
			tableQueryEngine = TableQueryEngineFactory.builder().eventBus(eventBus).factories(factories).build();
		}

		this.factories = factories;
		this.eventBus = eventBus;
		this.tableQueryEngine = tableQueryEngine;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("engineId", engineId).toString();
	}

	@Override
	public IOperatorFactory getOperatorFactory() {
		return factories.getOperatorFactory();
	}

	@Override
	public ITabularView execute(QueryPod queryPod) {
		IStopwatch stopWatch = factories.getStopwatchFactory().createStarted();
		boolean postedAboutDone = false;
		try {
			postAboutQueryStart(queryPod);

			QueryStepsDag queryStepsDag = makeQueryStepsDag(queryPod);

			if (queryPod.isDebugOrExplain()) {
				explainDagSteps(queryPod, queryStepsDag);
			}

			// Bind any per-thread scopes required by the slice factory (e.g. ScopedValueAppendableTable) for the
			// whole DAG execution. No-op for ThreadLocal-backed factories. Each sub-cube spawned in a child
			// virtual thread re-establishes its own scope via its own CubeQueryEngine#execute.
			ITabularView tabularView = PodExecutors.runScoped(queryPod, () -> executeDag(queryPod, queryStepsDag));

			if (queryPod.isDebugOrExplain()) {
				explainDagPerfs(queryPod, queryStepsDag);
			}

			postAboutQueryDone(queryPod, "OK", stopWatch);
			postedAboutDone = true;
			return tabularView;
		} catch (RuntimeException e) {
			// TODO Add the Exception to the event
			postAboutQueryDone(queryPod, "KO", stopWatch);
			postedAboutDone = true;

			String eMsg = "Issue executing query=%s options=%s".formatted(queryPod.getQuery(), queryPod.getOptions());

			throw AdhocExceptionHelpers.wrap(eMsg, e);
		} finally {
			if (!postedAboutDone) {
				// This may happen in case of OutOfMemoryError, or any uncaught exception
				postAboutQueryDone(queryPod, "KO_Uncaught", stopWatch);
			}
		}
	}

	protected void postAboutQueryStart(QueryPod queryPod) {
		eventBus.post(AdhocLogEvent.builder()
				.message("Executing on table=%s forest=%s query=%s"
						.formatted(queryPod.getTable().getName(), queryPod.getForest().getName(), queryPod.getQuery()))
				.source(this)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_START)
				.build());

		eventBus.post(QueryLifecycleEvent.builder()
				.query(queryPod)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_START)
				.build());
	}

	protected void postAboutQueryDone(QueryPod queryPod, String status, IStopwatch stopWatch) {
		eventBus.post(AdhocLogEvent.builder()
				.message("Executed status=%s duration=%s on table=%s forest=%s query=%s".formatted(status,
						PepperLogHelper.humanDuration(stopWatch.elapsed().toMillis()),
						queryPod.getTable().getName(),
						queryPod.getForest().getName(),
						queryPod.getQuery()))
				.source(this)
				.performance(true)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_DONE)
				.build());

		eventBus.post(QueryLifecycleEvent.builder()
				.query(queryPod)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_DONE)
				.build());
	}

	protected void explainDagSteps(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		makeDagExplainer().explain(queryPod.getQueryId(), queryStepsDag);
	}

	protected DagExplainer makeDagExplainer() {
		return DagExplainer.builder().eventBus(eventBus).build();
	}

	protected void explainDagPerfs(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		makeDagExplainerForPerfs().explain(queryPod.getQueryId(), queryStepsDag);
	}

	protected DagExplainerForPerfs makeDagExplainerForPerfs() {
		return DagExplainerForPerfs.builder().eventBus(eventBus).build();
	}

	@VisibleForTesting
	public QueryStepsDag makeQueryStepsDag(QueryPod queryPod) {
		IQueryStepsDagBuilder queryStepsDagBuilder = makeQueryStepsDagsBuilder(queryPod);

		// Add explicitly requested steps
		Set<IMeasure> queriedMeasures = getRootMeasures(queryPod);

		long nbQueriedMeasures = queriedMeasures.stream().map(IMeasure::getName).distinct().count();
		if (nbQueriedMeasures < queriedMeasures.size()) {
			AtomicLongMap<String> nameToCount = AtomicLongMap.create();
			queriedMeasures.forEach(m -> nameToCount.incrementAndGet(m.getName()));
			// Remove not conflicting
			nameToCount.asMap().keySet().forEach(nameToCount::decrementAndGet);
			nameToCount.removeAllZeros();
			nameToCount.asMap().keySet().forEach(nameToCount::incrementAndGet);

			throw new IllegalArgumentException(
					"Can not query multiple measures with same name: %s".formatted(nameToCount));
		}

		queryStepsDagBuilder.registerRootWithDescendants(queriedMeasures);

		QueryStepsDag queryDag = queryStepsDagBuilder.getQueryDag();

		if (queryPod.getOptions().contains(StandardQueryOptions.DRILLTHROUGH)) {
			// IQueryStepsDagBuilder dtQueryStepsDagBuilder = makeQueryStepsDagsBuilder(queryPod);
			// dtQueryStepsDagBuilder.
			// return dtQueryStepsDagBuilder.getQueryDag();

			// dag.outgoingEdgesOf(induced).stream().map(dag::getEdgeTarget).toList();
			// Inducers are tableQueries
			ImmutableSet<CubeQueryStep> inducers = queryDag.getInducers();

			final IAdhocDag<CubeQueryStep> dag = new AdhocDag<>();
			final DirectedMultigraph<CubeQueryStep, DefaultEdge> multigraph =
					new DirectedMultigraph<>(DefaultEdge.class);

			inducers.forEach(s -> {
				dag.addVertex(s);
				multigraph.addVertex(s);
			});

			// Similar to eu.solven.adhoc.engine.IQueryStepsDagBuilder.getQueryDag()
			return QueryStepsDag.builder()
					.inducedToInducer(dag)
					.multigraph(multigraph)
					.explicits(inducers)
					.stepToValues(queryDag.getStepToValues())
					.build();
		}

		return queryDag;
	}

	/**
	 * This is especially important to manage the case where no measure is requested, and we have to add some default
	 * measure.
	 * 
	 * @param queryPod
	 * @return the {@link Set} of root measures
	 */
	protected Set<IMeasure> getRootMeasures(QueryPod queryPod) {
		Set<IMeasure> measures = queryPod.getQuery().getMeasures();
		Set<IMeasure> queriedMeasures;
		if (measures.isEmpty()) {
			IMeasure defaultMeasure = defaultMeasure();
			queriedMeasures = ImmutableSet.of(defaultMeasure);
		} else {
			queriedMeasures = measures.stream().peek(m -> {
				if (emptyMeasureName.equals(m.getName())) {
					throw new IllegalArgumentException("The defaultEmptyMeasure can not be requested explicitly");
				}
			}).collect(ImmutableSet.toImmutableSet());
		}
		return queriedMeasures;
	}

	/**
	 * This measure is used to materialize slices. Typically used to list coordinates along a column.
	 * 
	 * @return the measure to be considered if not measure is provided to the query
	 */
	protected IMeasure defaultMeasure() {
		return Aggregator.empty().toBuilder().name(emptyMeasureName).build();
	}

	protected IQueryStepsDagBuilder makeQueryStepsDagsBuilder(QueryPod queryPod) {
		return QueryStepsDagBuilder.make(factories, queryPod);
	}

	protected ITabularView executeDag(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		// bootstrap is a fake empty phase
		// It is relevant to have a log before the `opening` phase which may be slow
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("bootstrap").source(this).build());

		if (StandardQueryOptions.DRILLTHROUGH.isActive(queryPod.getOptions())) {
			// DRILLTHROUGH bypasses measure aggregation: every DB row becomes a tabular entry directly.
			ITabularView drillthroughView = executeDrillthrough(queryPod, queryStepsDag);
			eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregate").source(this).build());
			eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transform").source(this).build());
			eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("view").source(this).build());
			return drillthroughView;
		}

		// Execute the leaf aggregations, by tableWrappers
		Map<CubeQueryStep, ICuboid> queryStepToValues = new ConcurrentHashMap<>();

		// Add values from cache
		queryStepToValues.putAll(queryStepsDag.getStepToValues());

		if (queryPod.isDebugOrExplain()) {
			log.info("[EXPLAIN] stepDag loaded {} steps from cache", queryStepToValues.size());
		}

		// Add values from table
		executeTableQueries(queryPod, queryStepsDag).forEach((tableStep, cuboid) -> {
			CubeQueryStep cubeStep = CubeQueryStep.edit(tableStep).build();
			ICuboid previousCuboid = queryStepToValues.put(cubeStep, cuboid);
			if (previousCuboid != null) {
				log.warn(
						"conflict on cubeStep from tableStep={} to cubeStep={} led to cuboid previous.size=%s vs new.size=%s",
						tableStep,
						cubeStep,
						previousCuboid.size(),
						cuboid.size());
			}
		});

		if (queryPod.isDebugOrExplain()) {
			log.info("[EXPLAIN] stepDag has {} steps post tableStep", queryStepToValues.size());
		}

		// We're done with the input stream: the DB can be shutdown, we can answer the
		// rest of the query independently of external tables.
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregate").source(this).build());

		walkUpDag(queryPod, queryStepsDag, queryStepToValues);

		registerResultsToCache(queryPod.getQueryStepCache(), queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transform").source(this).build());

		ITabularView tabularView = toTabularView(queryPod, queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("view").source(this).build());

		return tabularView;
	}

	protected void registerResultsToCache(IQueryStepCache queryStepCache,
			QueryStepsDag queryStepsDag,
			Map<CubeQueryStep, ICuboid> queryStepToValues) {
		// TODO Improve policy to detect which node should be put in cache
		// Typically, we may prefer high-level measure not to go through large chunk of steps. We may also keep in cache
		// steps which were slow to be computed.
		// BEWARE This mono-threaded iteration helps pushing into cache with a specific order, given the `stepToValues`
		// is typically a ConcurrenthashMap, hence with indeterministic order.
		queryStepsDag.iteratorFromInducerToInduced().forEachRemaining(step -> {
			if (queryStepsDag.getStepToValues().containsKey(step)) {
				log.debug("Do not add an entry already in cache");
				// Else it may force keeping the entry
			} else {
				SizeAndDuration cost = queryStepsDag.getStepToCost().get(step);
				ICuboid value = queryStepToValues.get(step);
				if (value == null) {
					log.debug("This happens in {}", StandardQueryOptions.DRILLTHROUGH);
				} else {
					queryStepCache.pushValue(step, value, cost);
				}
			}
		});
	}

	protected Map<TableQueryStep, ICuboid> executeTableQueries(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		// queryPod.with
		// AdhocSubQuery.builder().subQuery(qu).parentQueryId(queryPod.getQueryId()).build();
		return tableQueryEngine.executeTableQueries(queryPod, queryStepsDag);
	}

	/**
	 * Execute the {@code queryStepsDag} in DRILLTHROUGH mode: returns raw table rows as a
	 * {@link eu.solven.adhoc.dataframe.tabular.ListMapEntryBasedTabularViewDrillThrough}, bypassing any cube-level
	 * measure transformation.
	 *
	 * @return a {@link ITabularView} of the raw rows.
	 */
	protected ITabularView executeDrillthrough(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		return tableQueryEngine.executeDrillthrough(queryPod, queryStepsDag);
	}

	/**
	 *
	 * @param queryStep
	 * @return a simplistic version of the queryStep, for logging purposes
	 */
	protected String simplistic(CubeQueryStep queryStep) {
		return queryStep.getMeasure().getName();
	}

	/**
	 *
	 * @param queryStep
	 * @return a dense version of the queryStep, for logging purposes
	 */
	protected String dense(CubeQueryStep queryStep) {
		// Do not log about debug, explain or cache
		return new StringBuilder().append("m=")
				.append(queryStep.getMeasure().getName())
				.append(" filter=")
				.append(queryStep.getFilter())
				.append(" groupBy=")
				.append(queryStep.getGroupBy())
				.append(" custom=")
				.append(queryStep.getCustomMarker())
				.toString();
	}

	protected void walkUpDag(QueryPod queryPod,
			QueryStepsDag queryStepsDag,
			Map<CubeQueryStep, ICuboid> queryStepToValues) {
		if (StandardQueryOptions.DRILLTHROUGH.isActive(queryPod.getOptions())) {
			// In case of drillthrough, we do not process any measure
			return;
		}

		Consumer<? super CubeQueryStep> queryStepConsumer = queryStep -> {
			try {
				onQueryStep(queryPod, queryStepsDag, queryStepToValues, queryStep);
			} catch (RuntimeException e) {
				throw AdhocExceptionHelpers.wrap("Issue processing step=%s".formatted(queryStep), e);
			}
		};

		QueryEngineConcurrencyHelper.walkUpDag(queryPod, queryStepsDag, queryStepToValues, queryStepConsumer);
	}

	protected void onQueryStep(QueryPod queryPod,
			QueryStepsDag queryStepsDag,
			Map<CubeQueryStep, ICuboid> queryStepToValues,
			CubeQueryStep step) {
		if (queryStepToValues.containsKey(step)) {
			// This typically happens on aggregator measures, as they are fed in a previous
			// step. Here, we want to process a measure once its underlying steps are completed
			return;
		} else if (step.getMeasure() instanceof Aggregator a) {
			throw new IllegalStateException("Missing a=%s cuboid for %s".formatted(a, step));
		}

		eventBus.post(QueryStepIsEvaluating.builder().queryStep(step).source(this).build());

		IMeasure measure = queryPod.resolveIfRef(step.getMeasure());

		IStopwatch stopWatch = factories.getStopwatchFactory().createStarted();
		Optional<ICuboid> optFromCache = Optional.ofNullable(queryStepsDag.getStepToValues().get(step));
		ICuboid outputColumn = optFromCache.orElseGet(() -> {
			List<CubeQueryStep> underlyingSteps = queryStepsDag.underlyingSteps(step);
			try {
				return processDagStep(queryStepToValues, step, underlyingSteps, measure);
			} catch (RuntimeException e) {
				throw rethrowWithDetails(step, queryStepsDag, e);
			}
		});

		Duration elapsed = stopWatch.elapsed();
		eventBus.post(QueryStepIsCompleted.builder()
				.querystep(step)
				.nbCells(outputColumn.size())
				.source(this)
				.duration(elapsed)
				.build());
		queryStepsDag.registerExecutionFeedback(step,
				SizeAndDuration.builder().size(outputColumn.size()).duration(elapsed).build());

		ICuboid alreadyIn = queryStepToValues.putIfAbsent(step, outputColumn);
		if (null != alreadyIn) {
			// This may happen only if CONCURRENT options is on, as a queryStep may be requested concurrently by
			// dependents.
			log.warn("A queryStep has been computed multiple times queryStep={}. Should not happen since 0.0.14",
					queryPod);
		}
	}

	protected boolean mayHoldCarriers(CubeQueryStep step) {
		return step.getMeasure() instanceof IHasAggregationKey agg
				&& factories.getOperatorFactory().makeAggregation(agg) instanceof IAggregationCarrier.IHasCarriers;
	}

	protected ICuboid processDagStep(Map<CubeQueryStep, ICuboid> queryStepToValues,
			CubeQueryStep queryStep,
			List<CubeQueryStep> underlyingSteps,
			IMeasure measure) {
		if (underlyingSteps.isEmpty()) {
			// This may happen on a Columnator which is missing a required column
			// TODO This prevent an IMeasure to generate slices from an empty list of underlyings
			return Cuboid.empty();
		} else if (measure instanceof IHasUnderlyingMeasures hasUnderlyingMeasures) {
			return processDagStep(queryStepToValues, queryStep, underlyingSteps, hasUnderlyingMeasures);
		} else {
			throw new UnsupportedOperationException("m=%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
		}
	}

	protected ICuboid processDagStep(Map<CubeQueryStep, ICuboid> queryStepToValues,
			CubeQueryStep queryStep,
			List<CubeQueryStep> underlyingSteps,
			IHasUnderlyingMeasures hasUnderlyingMeasures) {
		List<ICuboid> underlyings = getUnderlyingColumns(queryStepToValues, underlyingSteps);

		// BEWARE The need to call again `.wrapNode` looks weird
		IMeasureQueryStep measureQuerySteps =
				factories.getMeasureQueryStepFactory().makeQueryStep(queryStep, hasUnderlyingMeasures);

		ICuboid coordinatesToValues;
		try {
			coordinatesToValues = measureQuerySteps.produceOutputColumn(underlyings);
		} catch (RuntimeException e) {
			if (StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE.isActive(queryStep.getOptions())) {
				IMultitypeColumnFastGet<ISlice> column = MultitypeHashColumn.<ISlice>builder().build();

				ISlice errorSlice = makeErrorSlice(queryStep, e);
				column.append(errorSlice).onObject(e);
				coordinatesToValues = Cuboid.forGroupBy(queryStep).values(column).build();
			} else {
				throw AdhocExceptionHelpers.wrap("Issue processing queryStep=%s".formatted(queryStep), e);
			}
		}

		// It feels a bit weird to compact after hand: ICuboid may be compacted by construction. But it is
		// unclear how to achieve that while guaranteed all ITransformatorQueryStep implements it effectively.
		coordinatesToValues.compact();

		return coordinatesToValues;
	}

	// TODO We should ensure this slice is valid given current filter
	protected ISlice makeErrorSlice(CubeQueryStep queryStep, RuntimeException e) {
		IMapBuilderPreKeys errorSliceAsMapBuilder = factories.getSliceFactoryFactory()
				.makeFactory(HasOptionsAndExecutorService.builder().options(queryStep.getOptions()).build())
				.newMapBuilder(queryStep.getGroupBy().getSortedColumns());
		queryStep.getGroupBy().getSortedColumns().forEach(groupedByColumn -> {
			String coordinateForError = e.getClass().getName();

			Object errorCoordinate;
			if (FilterHelpers.getValueMatcher(queryStep.getFilter(), groupedByColumn).match(coordinateForError)) {
				errorCoordinate = coordinateForError;
			} else {
				IValueMatcher valueMatcher = FilterHelpers.getValueMatcher(queryStep.getFilter(), groupedByColumn);
				if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
					errorCoordinate = equalsMatcher.getOperand();
				} else if (valueMatcher instanceof StringMatcher stringMatcher) {
					errorCoordinate = stringMatcher.getString();
				} else {
					throw new NotYetImplementedException(
							"valueMatcher=%s in step=%s".formatted(valueMatcher, queryStep),
							e);
				}
			}

			errorSliceAsMapBuilder.append(errorCoordinate);
		});

		return errorSliceAsMapBuilder.build().asSlice();
	}

	protected List<ICuboid> getUnderlyingColumns(Map<CubeQueryStep, ICuboid> queryStepToValues,
			List<CubeQueryStep> underlyingSteps) {
		return underlyingSteps.stream().map(underlyingStep -> {
			ICuboid underlyingValues = queryStepToValues.get(underlyingStep);

			if (underlyingValues == null) {
				if (underlyingStep.getMeasure() instanceof EmptyMeasure) {
					return Cuboid.empty();
				} else {
					throw new IllegalStateException("missing underlyingStep=%s".formatted(underlyingStep));
				}
			}

			if (mayHoldCarriers(underlyingStep)) {
				return underlyingValues.purgeCarriers();
			} else {
				return underlyingValues;
			}
		}).toList();
	}

	@SuppressWarnings({ "PMD.InsufficientStringBufferDeclaration",
			"PMD.ConsecutiveAppendsShouldReuse",
			"checkstyle:MagicNumber" })
	protected IllegalStateException rethrowWithDetails(CubeQueryStep queryStep,
			QueryStepsDag queryStepsDag,
			RuntimeException e) {
		StringBuilder describeStep = new StringBuilder();

		describeStep.append("Issue computing columns for:").append(System.lineSeparator());

		List<CubeQueryStep> underlyingSteps = queryStepsDag.underlyingSteps(queryStep);

		// First, we print only measure as a simplistic shorthand of the step
		describeStep.append("    (measures) m=%s given %s".formatted(simplistic(queryStep),
				underlyingSteps.stream().map(this::simplistic).toList())).append(System.lineSeparator());
		// Second, we print the underlying steps as something may be hidden in filters, groupBys, configuration
		describeStep.append("    (steps) step=%s given %s".formatted(dense(queryStep),
				underlyingSteps.stream().map(this::dense).toList())).append(System.lineSeparator());

		IAdhocDag<CubeQueryStep> inducedToInducers = queryStepsDag.getInducedToInducer();
		if (inducedToInducers.edgeSet().size() > 1024) {
			// `shortestPaths.getPaths` may consume a lot of RAM: we skip this step if the DAG is too big
			describeStep.append("#steps=").append(inducedToInducers.edgeSet().size());
		} else {
			ShortestPathAlgorithm<CubeQueryStep, DefaultEdge> shortestPaths =
					new JohnsonShortestPaths<>(inducedToInducers);

			queryStepsDag.getExplicits()
					.stream()
					.map(queriedStep -> shortestPaths.getPaths(queriedStep).getPath(queryStep))
					.filter(Objects::nonNull)
					// Return the shorted path, as it is the simpler to analyze by a human
					.min(Comparator.comparing(gp -> gp.getVertexList().size()))
					.ifPresent(shortestPath -> {
						describeStep.append("Path from root:");

						List<CubeQueryStep> vertexList = shortestPath.getVertexList();
						for (int i = 0; i < vertexList.size(); i++) {
							describeStep.append(System.lineSeparator());
							IntStream.range(0, i).forEach(tabIndex -> describeStep.append('\t'));
							describeStep.append("\\-").append(vertexList.get(i));
						}
					});
		}

		return new IllegalStateException(describeStep.toString(), e);
	}

	/**
	 * This step will align the different measure on a per-slice basis.
	 * 
	 * @param queryPod
	 * @param queryStepsDag
	 * @param queryStepToValues
	 * @return the output {@link ITabularView}
	 */
	protected ITabularView toTabularView(QueryPod queryPod,
			QueryStepsDag queryStepsDag,
			Map<CubeQueryStep, ICuboid> queryStepToValues) {
		if (queryStepToValues.isEmpty()) {
			return MapBasedTabularView.empty();
		}

		// BEWARE some queriedStep may be in the middle of the DAG if it is also the underlying of another step
		Iterator<CubeQueryStep> stepsToReturn = queryStepsDag.getExplicits().iterator();
		long expectedOutputCardinality = queryStepToValues.values().stream().mapToLong(ICuboid::size).max().getAsLong();

		ITabularView view = makeTabularView(queryPod, expectedOutputCardinality);

		stepsToReturn.forEachRemaining(step -> {
			ICuboid coordinatesToValues = queryStepToValues.get(step);
			if (coordinatesToValues == null) {
				// Happens on a Columnator missing a required column
				log.debug("No cuboid for step={}", step);
			} else {
				boolean isEmptyMeasure = step.getMeasure() instanceof Aggregator agg && EmptyAggregation.isEmpty(agg);

				boolean doClearCarriers = mayHoldCarriers(step)
						&& !StandardQueryOptions.AGGREGATION_CARRIERS_STAY_WRAPPED.isActive(queryPod.getOptions());

				IColumnScanner<ISlice> baseRowScanner =
						slice -> view.sliceFeeder(slice, step.getMeasure().getName(), isEmptyMeasure);

				IColumnScanner<ISlice> rowScanner =
						scannerForTabularView(isEmptyMeasure, doClearCarriers, baseRowScanner);
				coordinatesToValues.forEachSlice(rowScanner);
			}
		});
		return view;
	}

	private ITabularView makeTabularView(QueryPod queryPod, long expectedOutputCardinality) {
		if (queryPod.getOptions().contains(StandardQueryOptions.DRILLTHROUGH)) {
			return ListMapEntryBasedTabularViewDrillThrough.withCapacity(expectedOutputCardinality);
		} else {
			return MapBasedTabularView.builder()
					// Force a HashMap not to rely on default TreeMap
					.coordinatesToValues(LinkedHashMap.newLinkedHashMap(Ints.saturatedCast(expectedOutputCardinality)))
					.build();
		}
	}

	protected IColumnScanner<ISlice> scannerForTabularView(boolean isEmptyMeasure,
			boolean doClearCarriers,
			IColumnScanner<ISlice> baseRowScanner) {
		IColumnScanner<ISlice> rowScanner;
		if (isEmptyMeasure) {
			rowScanner = slice -> {
				IValueReceiver sliceFeeder = baseRowScanner.onKey(slice);

				// `emptyValue` may not empty as we needed to materialize it to get up to here
				// But now is time to force it to null, while materializing the slice.
				return new IValueReceiver() {

					// This is useful to prevent boxing emptyValue when long
					@Override
					public void onLong(long v) {
						sliceFeeder.onObject(null);
					}

					// This is useful to prevent boxing emptyValue when double
					@Override
					public void onDouble(double v) {
						sliceFeeder.onObject(null);
					}

					@Override
					public void onObject(Object v) {
						sliceFeeder.onObject(null);
					}
				};
			};
		} else if (doClearCarriers) {
			rowScanner = slice -> {
				IValueReceiver sliceFeeder = baseRowScanner.onKey(slice);

				// `emptyValue` may not empty as we needed to materialize it to get up to here
				// But now is time to force it to null, while materializing the slice.
				return new IValueReceiver() {

					// This is useful to prevent boxing emptyValue when long
					@Override
					public void onLong(long v) {
						sliceFeeder.onLong(v);
					}

					// This is useful to prevent boxing emptyValue when double
					@Override
					public void onDouble(double v) {
						sliceFeeder.onDouble(v);
					}

					@Override
					public void onObject(Object v) {
						if (v instanceof IValueProvider valueProvider) {
							// Transfer the carried value
							valueProvider.acceptReceiver(sliceFeeder);
						} else {
							sliceFeeder.onObject(v);
						}
					}
				};
			};
		} else {
			rowScanner = baseRowScanner;
		}
		return rowScanner;
	}

}
