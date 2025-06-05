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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
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
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.operator.IHasOperatorsFactory;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorsFactory;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.util.IAdhocEventBus;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.adhoc.util.IStopwatchFactory;
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
public class CubeQueryEngine implements ICubeQueryEngine, IHasOperatorsFactory {
	private final UUID engineId = UUID.randomUUID();

	// This shall not conflict with any user measure
	private final String emptyMeasureName = "$ADHOC$empty-" + engineId;

	@NonNull
	@Default
	@Getter
	final IOperatorsFactory operatorsFactory = new StandardOperatorsFactory();

	@NonNull
	@Default
	// @Getter is useful for tests. May be useful to help providing a relevant EventBus to other components.
	@Getter
	final IAdhocEventBus eventBus = IAdhocEventBus.BLACK_HOLE;

	@NonNull
	@Default
	IStopwatchFactory stopwatchFactory = IStopwatchFactory.guavaStopwatchFactory();

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "=" + engineId;
	}

	@Override
	public ITabularView execute(QueryPod queryPod) {
		IStopwatch stopWatch = stopwatchFactory.createStarted();
		boolean postedAboutDone = false;
		try {
			postAboutQueryStart(queryPod);

			QueryStepsDag queryStepsDag = makeQueryStepsDag(queryPod);

			if (queryPod.isExplain() || queryPod.isDebug()) {
				explainDagSteps(queryPod, queryStepsDag);
			}

			ITabularView tabularView = executeDag(queryPod, queryStepsDag);

			if (queryPod.isExplain() || queryPod.isDebug()) {
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

	protected void explainDagSteps(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		makeDagExplainer().explain(queryPod.getQueryId(), queryStepsDag);
	}

	protected void explainDagPerfs(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		makeDagExplainerForPerfs().explain(queryPod.getQueryId(), queryStepsDag);
	}

	protected DagExplainer makeDagExplainer() {
		return DagExplainer.builder().eventBus(eventBus).build();
	}

	protected DagExplainerForPerfs makeDagExplainerForPerfs() {
		return DagExplainerForPerfs.builder().eventBus(eventBus).build();
	}

	protected QueryStepsDag makeQueryStepsDag(QueryPod queryPod) {
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

		queryStepsDagBuilder.registerRootWithDescendants(queryPod::resolveIfRef, queriedMeasures);

		return queryStepsDagBuilder.getQueryDag();
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
		return new QueryStepsDagBuilder(operatorsFactory, queryPod.getTable().getName(), queryPod.getQuery());
	}

	protected ITabularView executeDag(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		// bootstrap is a fake empty phase
		// It is relevant to have a log before the `opening` phase which may be slow
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("bootstrap").source(this).build());

		// Execute the leaf aggregations, by tableWrappers
		Map<CubeQueryStep, ISliceToValue> queryStepToValues = executeTableQueries(queryPod, queryStepsDag);

		// We're done with the input stream: the DB can be shutdown, we can answer the
		// rest of the query independently of external tables.
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregate").source(this).build());

		walkDagUpToQueriedMeasures(queryPod, queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transform").source(this).build());

		ITabularView tabularView = toTabularView(queryPod, queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("view").source(this).build());

		return tabularView;
	}

	protected Map<CubeQueryStep, ISliceToValue> executeTableQueries(QueryPod queryPod, QueryStepsDag queryStepsDag) {
		ITableQueryEngine tableQueryEngine = makeTableQueryEngine();
		return tableQueryEngine.executeTableQueries(queryPod, queryStepsDag);
	}

	protected TableQueryEngine makeTableQueryEngine() {
		return TableQueryEngine.builder()
				.eventBus(eventBus)
				.operatorsFactory(operatorsFactory)
				.stopwatchFactory(stopwatchFactory)
				.build();
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
		// Do not log about debug, explainm or cache
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

	protected void walkDagUpToQueriedMeasures(QueryPod queryPod,
			QueryStepsDag queryStepsDag,
			Map<CubeQueryStep, ISliceToValue> queryStepToValues) {

		try {
			queryPod.getExecutorService().submit(() -> {
				Stream<CubeQueryStep> topologicalOrder = queryStepsDag.fromAggregatesToQueried();

				if (queryPod.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
					topologicalOrder = topologicalOrder.parallel();
				}

				topologicalOrder.forEach(queryStep -> {
					if (queryStepToValues.containsKey(queryStep)) {
						// This typically happens on aggregator measures, as they are fed in a previous
						// step. Here, we want to process a measure once its underlying steps are completed
						return;
					} else if (queryStep.getMeasure() instanceof Aggregator a) {
						throw new IllegalStateException("Missing values for %s".formatted(a));
					}

					eventBus.post(QueryStepIsEvaluating.builder().queryStep(queryStep).source(this).build());

					IMeasure measure = queryPod.resolveIfRef(queryStep.getMeasure());

					List<CubeQueryStep> underlyingSteps = queryStepsDag.underlyingSteps(queryStep);

					IStopwatch stepWatch = stopwatchFactory.createStarted();

					Optional<ISliceToValue> optOutputColumn =
							processDagStep(queryStepToValues, queryStep, underlyingSteps, measure);

					// Would be empty on table steps (leaves of the DAG)
					optOutputColumn.ifPresent(outputColumn -> {
						Duration elapsed = stepWatch.elapsed();
						eventBus.post(QueryStepIsCompleted.builder()
								.querystep(queryStep)
								.nbCells(outputColumn.size())
								.source(this)
								.duration(elapsed)
								.build());
						queryStepsDag.registerExecutionFeedback(queryStep,
								SizeAndDuration.builder().size(outputColumn.size()).duration(elapsed).build());

						queryStepToValues.put(queryStep, outputColumn);
					});
				});
			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted", e);
		} catch (ExecutionException e) {
			throw new IllegalStateException("Failed", e);
		}
	}

	protected Optional<ISliceToValue> processDagStep(Map<CubeQueryStep, ISliceToValue> queryStepToValues,
			CubeQueryStep queryStep,
			List<CubeQueryStep> underlyingSteps,
			IMeasure measure) {
		if (underlyingSteps.isEmpty()) {
			// This may happen on a IMeasure which is missing a required column
			return Optional.empty();
		} else if (measure instanceof IHasUnderlyingMeasures hasUnderlyingMeasures) {
			List<ISliceToValue> underlyings = underlyingSteps.stream().map(underlyingStep -> {
				ISliceToValue values = queryStepToValues.get(underlyingStep);

				if (values == null) {
					if (underlyingStep.getMeasure() instanceof EmptyMeasure) {
						return SliceToValue.empty();
					} else {
						throw new IllegalStateException("The DAG missed values for step=%s".formatted(underlyingStep));
					}
				}

				return values;
			}).toList();

			// BEWARE It looks weird we have to call again `.wrapNode`
			ITransformatorQueryStep transformatorQuerySteps =
					hasUnderlyingMeasures.wrapNode(operatorsFactory, queryStep);

			ISliceToValue coordinatesToValues;
			try {
				coordinatesToValues = transformatorQuerySteps.produceOutputColumn(underlyings);
			} catch (RuntimeException e) {
				throw rethrowWithDetails(queryStep, underlyingSteps, e);
			}

			return Optional.of(coordinatesToValues);
		} else {
			throw new UnsupportedOperationException("%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
		}
	}

	@SuppressWarnings({ "PMD.InsufficientStringBufferDeclaration",
			"PMD.ConsecutiveAppendsShouldReuse",
			"PMD.ConsecutiveLiteralAppends" })
	protected IllegalStateException rethrowWithDetails(CubeQueryStep queryStep,
			List<CubeQueryStep> underlyingSteps,
			RuntimeException e) {
		StringBuilder describeStep = new StringBuilder();

		describeStep.append("Issue computing columns for:").append("\r\n");

		// First, we print only measure as a simplistic shorthand of the step
		describeStep.append("    (measures) m=%s given %s".formatted(simplistic(queryStep),
				underlyingSteps.stream().map(this::simplistic).toList())).append("\r\n");
		// Second, we print the underlying steps as something may be hidden in filters, groupBys, configuration
		describeStep.append("    (steps) step=%s given %s".formatted(dense(queryStep),
				underlyingSteps.stream().map(this::dense).toList())).append("\r\n");

		return new IllegalStateException(describeStep.toString(), e);
	}

	/**
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
		Iterator<CubeQueryStep> stepsToReturn = queryStepsDag.getQueried().iterator();
		long expectedOutputCardinality =
				queryStepToValues.values().stream().mapToLong(ISliceToValue::size).max().getAsLong();

		MapBasedTabularView mapBasedTabularView = MapBasedTabularView.builder()
				// Force a HashMap not to rely on default TreeMap
				.coordinatesToValues(LinkedHashMap.newLinkedHashMap(Ints.saturatedCast(expectedOutputCardinality)))
				.build();

		stepsToReturn.forEachRemaining(step -> {
			ISliceToValue coordinatesToValues = queryStepToValues.get(step);
			if (coordinatesToValues == null) {
				// Happens on a Columnator missing a required column
				log.debug("No sliceToValue for step={}", step);
			} else {
				boolean isEmptyMeasure = step.getMeasure() instanceof Aggregator agg
						&& EmptyAggregation.isEmpty(agg.getAggregationKey());

				boolean hasCarrierMeasure = step.getMeasure() instanceof Aggregator agg
						&& operatorsFactory.makeAggregation(agg) instanceof IAggregationCarrier.IHasCarriers
						&& !queryPod.getOptions().contains(StandardQueryOptions.AGGREGATION_CARRIERS_STAY_WRAPPED);

				IColumnScanner<SliceAsMap> baseRowScanner =
						slice -> mapBasedTabularView.sliceFeeder(slice, step.getMeasure().getName(), isEmptyMeasure);

				IColumnScanner<SliceAsMap> rowScanner;
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
				} else if (hasCarrierMeasure) {
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
				coordinatesToValues.forEachSlice(rowScanner);
			}
		});
		return mapBasedTabularView;
	}

}
