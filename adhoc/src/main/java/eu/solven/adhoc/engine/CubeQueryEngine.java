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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jgrapht.alg.interfaces.ShortestPathAlgorithm;
import org.jgrapht.alg.shortestpath.JohnsonShortestPaths;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.DirectedMultigraph;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.ListMapEntryBasedTabularViewDrillThrough;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.observability.AdhocQueryMonitor;
import eu.solven.adhoc.engine.observability.DagExplainer;
import eu.solven.adhoc.engine.observability.DagExplainerForPerfs;
import eu.solven.adhoc.engine.observability.SizeAndDuration;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.tabular.ITableQueryEngine;
import eu.solven.adhoc.engine.tabular.TableQueryEngine;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.eventbus.UnsafeAdhocEventBusHelpers;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
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
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;
import eu.solven.adhoc.util.AdhocBlackHole;
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
	final AdhocFactories factories = AdhocFactories.builder().build();

	@NonNull
	@Default
	// @Getter is useful for tests. May be useful to help providing a relevant EventBus to other components.
	@Getter
	@SuppressWarnings("PMD.UnusedAssignment")
	final IAdhocEventBus eventBus = UnsafeAdhocEventBusHelpers.safeWrapper(AdhocBlackHole.getInstance());

	@NonNull
	@VisibleForTesting
	@Getter
	ITableQueryEngine tableQueryEngine;

	protected CubeQueryEngine(AdhocFactories factories, IAdhocEventBus eventBus, ITableQueryEngine tableQueryEngine) {
		if (tableQueryEngine == null) {
			tableQueryEngine = TableQueryEngine.builder().eventBus(eventBus).factories(factories).build();
		}

		this.factories = factories;
		this.eventBus = eventBus;
		this.tableQueryEngine = tableQueryEngine;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "=" + engineId;
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

			ITabularView tabularView = executeDag(queryPod, queryStepsDag);

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

			throw AdhocExceptionHelpers.wrap(e, eMsg);
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

	protected DagExplainer makeDagExplainer() {
		return DagExplainer.builder().eventBus(eventBus).build();
	}

	protected void explainDagSteps(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		makeDagExplainer().explain(queryPod.getQueryId(), queryStepsDag);
	}

	protected DagExplainerForPerfs makeDagExplainerForPerfs() {
		return DagExplainerForPerfs.builder().eventBus(eventBus).build();
	}

	protected void explainDagPerfs(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		makeDagExplainerForPerfs().explain(queryPod.getQueryId(), queryStepsDag);
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

			final DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
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
			queriedMeasures = Set.of(defaultMeasure);
		} else {
			queriedMeasures = measures.stream().peek(m -> {
				if (emptyMeasureName.equals(m.getName())) {
					throw new IllegalArgumentException("The defaultEmptyMeasure can not be requested explicitly");
				}
			}).collect(Collectors.toSet());
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

		// Execute the leaf aggregations, by tableWrappers
		Map<CubeQueryStep, ISliceToValue> queryStepToValues = new ConcurrentHashMap<>();

		// Add values from cache
		queryStepToValues.putAll(queryStepsDag.getStepToValues());

		// Add values from table
		queryStepToValues.putAll(executeTableQueries(queryPod, queryStepsDag));

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
			Map<CubeQueryStep, ISliceToValue> queryStepToValues) {
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
				ISliceToValue value = queryStepToValues.get(step);
				if (value == null) {
					log.debug("This happens in {}", StandardQueryOptions.DRILLTHROUGH);
				} else {
					queryStepCache.pushValue(step, value, cost);
				}
			}
		});
	}

	protected Map<CubeQueryStep, ISliceToValue> executeTableQueries(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		// queryPod.with
		// AdhocSubQuery.builder().subQuery(qu).parentQueryId(queryPod.getQueryId()).build();
		return tableQueryEngine.executeTableQueries(queryPod, queryStepsDag);
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
			Map<CubeQueryStep, ISliceToValue> queryStepToValues) {
		if (queryPod.getOptions().contains(StandardQueryOptions.DRILLTHROUGH)) {
			// In case of drillthrough, we do not process any measure
			return;
		}

		Consumer<? super CubeQueryStep> queryStepConsumer = queryStep -> {
			try {
				onQueryStep(queryPod, queryStepsDag, queryStepToValues, queryStep);
			} catch (RuntimeException e) {
				throw AdhocExceptionHelpers.wrap(e, "Issue processing step=%s".formatted(queryStep));
			}
		};

		QueryEngineConcurrencyHelper.walkUpDag(queryPod, queryStepsDag, queryStepToValues, queryStepConsumer);
	}

	protected void onQueryStep(QueryPod queryPod,
			QueryStepsDag queryStepsDag,
			Map<CubeQueryStep, ISliceToValue> queryStepToValues,
			CubeQueryStep queryStep) {
		if (queryStepToValues.containsKey(queryStep)) {
			// This typically happens on aggregator measures, as they are fed in a previous
			// step. Here, we want to process a measure once its underlying steps are completed
			return;
		} else if (queryStep.getMeasure() instanceof Aggregator a) {
			throw new IllegalStateException("Missing values for %s".formatted(a));
		}

		eventBus.post(QueryStepIsEvaluating.builder().queryStep(queryStep).source(this).build());

		IMeasure measure = queryPod.resolveIfRef(queryStep.getMeasure());

		IStopwatch stopWatch = factories.getStopwatchFactory().createStarted();
		Optional<ISliceToValue> optFromCache = Optional.ofNullable(queryStepsDag.getStepToValues().get(queryStep));
		ISliceToValue outputColumn = optFromCache.orElseGet(() -> {
			List<CubeQueryStep> underlyingSteps = queryStepsDag.underlyingSteps(queryStep);
			try {
				return processDagStep(queryStepToValues, queryStep, underlyingSteps, measure);
			} catch (RuntimeException e) {
				throw rethrowWithDetails(queryStep, queryStepsDag, e);
			}
		});

		Duration elapsed = stopWatch.elapsed();
		eventBus.post(QueryStepIsCompleted.builder()
				.querystep(queryStep)
				.nbCells(outputColumn.size())
				.source(this)
				.duration(elapsed)
				.build());
		queryStepsDag.registerExecutionFeedback(queryStep,
				SizeAndDuration.builder().size(outputColumn.size()).duration(elapsed).build());

		ISliceToValue alreadyIn = queryStepToValues.putIfAbsent(queryStep, outputColumn);
		if (null != alreadyIn) {
			// This may happen only if CONCURRENT options is on, as a queryStep may be requested concurrently by
			// dependents.
			// TODO Prevent an intermediate step to be computed multiple times
			log.debug("A queryStep has been computed multiple times queryStep={}", queryPod);
		}
	}

	protected boolean mayHoldCarriers(CubeQueryStep step) {
		return step.getMeasure() instanceof IHasAggregationKey agg
				&& factories.getOperatorFactory().makeAggregation(agg) instanceof IAggregationCarrier.IHasCarriers;
	}

	protected ISliceToValue processDagStep(Map<CubeQueryStep, ISliceToValue> queryStepToValues,
			CubeQueryStep queryStep,
			List<CubeQueryStep> underlyingSteps,
			IMeasure measure) {
		if (underlyingSteps.isEmpty()) {
			// This may happen on a Columnator which is missing a required column
			// TODO This prevent an IMeasure to generate slices from an empty list of underlyings
			return SliceToValue.empty();
		} else if (measure instanceof IHasUnderlyingMeasures hasUnderlyingMeasures) {
			return processDagStep(queryStepToValues, queryStep, underlyingSteps, hasUnderlyingMeasures);
		} else {
			throw new UnsupportedOperationException("m=%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
		}
	}

	protected ISliceToValue processDagStep(Map<CubeQueryStep, ISliceToValue> queryStepToValues,
			CubeQueryStep queryStep,
			List<CubeQueryStep> underlyingSteps,
			IHasUnderlyingMeasures hasUnderlyingMeasures) {
		List<ISliceToValue> underlyings = getUnderlyingColumns(queryStepToValues, underlyingSteps);

		// BEWARE The need to call again `.wrapNode` looks weird
		ITransformatorQueryStep transformatorQuerySteps = hasUnderlyingMeasures.wrapNode(factories, queryStep);

		ISliceToValue coordinatesToValues;
		try {
			coordinatesToValues = transformatorQuerySteps.produceOutputColumn(underlyings);
		} catch (RuntimeException e) {
			if (queryStep.getOptions().contains(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)) {
				IMultitypeColumnFastGet<IAdhocSlice> column = MultitypeHashColumn.<IAdhocSlice>builder().build();

				IAdhocSlice errorSlice = makeErrorSlice(queryStep, e);
				column.append(errorSlice).onObject(e);
				coordinatesToValues = SliceToValue.forGroupBy(queryStep).values(column).build();
			} else {
				throw AdhocExceptionHelpers.wrap(e, "Issue processing queryStep=%s".formatted(queryStep));
			}
		}

		// It feels a bit weird to compact after hand: ISliceToValue may be compacted by construction. But it is
		// unclear how to achieve that while guaranteed all ITransformatorQueryStep implements it effectively.
		coordinatesToValues.compact();

		return coordinatesToValues;
	}

	// TODO We should ensure this slice is valid given current filter
	protected IAdhocSlice makeErrorSlice(CubeQueryStep queryStep, RuntimeException e) {
		IMapBuilderPreKeys errorSliceAsMapBuilder =
				factories.getSliceFactory().newMapBuilder(queryStep.getGroupBy().getGroupedByColumns());
		queryStep.getGroupBy().getGroupedByColumns().forEach(groupedByColumn -> {
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

	protected List<ISliceToValue> getUnderlyingColumns(Map<CubeQueryStep, ISliceToValue> queryStepToValues,
			List<CubeQueryStep> underlyingSteps) {
		return underlyingSteps.stream().map(underlyingStep -> {
			ISliceToValue underlyingValues = queryStepToValues.get(underlyingStep);

			if (underlyingValues == null) {
				if (underlyingStep.getMeasure() instanceof EmptyMeasure) {
					return SliceToValue.empty();
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

		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> inducedToInducers = queryStepsDag.getInducedToInducer();
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
			Map<CubeQueryStep, ISliceToValue> queryStepToValues) {
		if (queryStepToValues.isEmpty()) {
			return MapBasedTabularView.empty();
		}

		// BEWARE some queriedStep may be in the middle of the DAG if it is also the underlying of another step
		Iterator<CubeQueryStep> stepsToReturn = queryStepsDag.getExplicits().iterator();
		long expectedOutputCardinality =
				queryStepToValues.values().stream().mapToLong(ISliceToValue::size).max().getAsLong();

		ITabularView mapBasedTabularView = makeTabularView(queryPod, expectedOutputCardinality);

		stepsToReturn.forEachRemaining(step -> {
			ISliceToValue coordinatesToValues = queryStepToValues.get(step);
			if (coordinatesToValues == null) {
				// Happens on a Columnator missing a required column
				log.debug("No sliceToValue for step={}", step);
			} else {
				boolean isEmptyMeasure = step.getMeasure() instanceof Aggregator agg && EmptyAggregation.isEmpty(agg);

				boolean doClearCarriers = mayHoldCarriers(step)
						&& !queryPod.getOptions().contains(StandardQueryOptions.AGGREGATION_CARRIERS_STAY_WRAPPED);

				IColumnScanner<IAdhocSlice> baseRowScanner =
						slice -> mapBasedTabularView.sliceFeeder(slice, step.getMeasure().getName(), isEmptyMeasure);

				IColumnScanner<IAdhocSlice> rowScanner =
						scannerForTabularView(isEmptyMeasure, doClearCarriers, baseRowScanner);
				coordinatesToValues.forEachSlice(rowScanner);
			}
		});
		return mapBasedTabularView;
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

	protected IColumnScanner<IAdhocSlice> scannerForTabularView(boolean isEmptyMeasure,
			boolean doClearCarriers,
			IColumnScanner<IAdhocSlice> baseRowScanner) {
		IColumnScanner<IAdhocSlice> rowScanner;
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
						if (v instanceof IAggregationCarrier aggregationCarrier) {
							// Transfer the carried value
							aggregationCarrier.acceptValueReceiver(sliceFeeder);
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
