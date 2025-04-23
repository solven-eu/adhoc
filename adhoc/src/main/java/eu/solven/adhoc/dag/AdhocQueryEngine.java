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
package eu.solven.adhoc.dag;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.primitives.Ints;

import eu.solven.adhoc.dag.context.ExecutingQueryContext;
import eu.solven.adhoc.dag.observability.AdhocQueryMonitor;
import eu.solven.adhoc.dag.observability.DagExplainer;
import eu.solven.adhoc.dag.observability.DagExplainerForPerfs;
import eu.solven.adhoc.dag.observability.SizeAndDuration;
import eu.solven.adhoc.dag.step.AdhocQueryStep;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.measure.IHasOperatorsFactory;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.ITransformator;
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
public class AdhocQueryEngine implements IAdhocQueryEngine, IHasOperatorsFactory {
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
	public ITabularView execute(ExecutingQueryContext executingQueryContext) {
		IStopwatch stopWatch = stopwatchFactory.createStarted();
		boolean postedAboutDone = false;
		try {
			postAboutQueryStart(executingQueryContext);

			QueryStepsDag queryStepsDag = makeQueryStepsDag(executingQueryContext);

			if (executingQueryContext.isExplain() || executingQueryContext.isDebug()) {
				explainDagSteps(executingQueryContext, queryStepsDag);
			}

			ITabularView tabularView = executeDag(executingQueryContext, queryStepsDag);

			if (executingQueryContext.isExplain() || executingQueryContext.isDebug()) {
				explainDagPerfs(executingQueryContext, queryStepsDag);
			}

			postAboutQueryDone(executingQueryContext, "OK", stopWatch);
			postedAboutDone = true;
			return tabularView;
		} catch (RuntimeException e) {
			// TODO Add the Exception to the event
			postAboutQueryDone(executingQueryContext, "KO", stopWatch);
			postedAboutDone = true;

			String eMsg = "Issue executing query=%s options=%s".formatted(executingQueryContext.getQuery(),
					executingQueryContext.getOptions());

			if (e instanceof IllegalStateException illegalStateE) {
				// We want to keep bubbling an IllegalStateException
				throw new IllegalStateException(eMsg, illegalStateE);
			} else if (e instanceof CompletionException completionE) {
				if (completionE.getCause() instanceof IllegalStateException) {
					// We want to keep bubbling an IllegalStateException
					throw new IllegalStateException(eMsg, completionE);
				} else {
					throw new IllegalArgumentException(eMsg, completionE);
				}
			} else {
				throw new IllegalArgumentException(eMsg, e);
			}
		} finally {
			if (!postedAboutDone) {
				// This may happen in case of OutOfMemoryError, or any uncaught exception
				postAboutQueryDone(executingQueryContext, "KO_Uncaught", stopWatch);
			}
		}
	}

	protected void postAboutQueryStart(ExecutingQueryContext executingQueryContext) {
		eventBus.post(AdhocLogEvent.builder()
				.message("Executing on table=%s measures=%s query=%s".formatted(executingQueryContext.getTable()
						.getName(), executingQueryContext.getForest().getName(), executingQueryContext.getQuery()))
				.source(this)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_START)
				.build());

		eventBus.post(QueryLifecycleEvent.builder()
				.query(executingQueryContext)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_START)
				.build());
	}

	protected void postAboutQueryDone(ExecutingQueryContext executingQueryContext,
			String status,
			IStopwatch stopWatch) {
		eventBus.post(AdhocLogEvent.builder()
				.message("Executed status=%s duration=%s on table=%s measures=%s query=%s".formatted(status,
						stopWatch.elapsed(),
						executingQueryContext.getTable().getName(),
						executingQueryContext.getForest().getName(),
						executingQueryContext.getQuery()))
				.source(this)
				.performance(true)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_DONE)
				.build());

		eventBus.post(QueryLifecycleEvent.builder()
				.query(executingQueryContext)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_DONE)
				.build());
	}

	protected void explainDagSteps(ExecutingQueryContext executingQueryContext, QueryStepsDag queryStepsDag) {
		makeDagExplainer().explain(executingQueryContext.getQueryId(), queryStepsDag);
	}

	protected void explainDagPerfs(ExecutingQueryContext executingQueryContext, QueryStepsDag queryStepsDag) {
		makeDagExplainerForPerfs().explain(executingQueryContext.getQueryId(), queryStepsDag);
	}

	protected DagExplainer makeDagExplainer() {
		return DagExplainer.builder().eventBus(eventBus).build();
	}

	protected DagExplainerForPerfs makeDagExplainerForPerfs() {
		return DagExplainerForPerfs.builder().eventBus(eventBus).build();
	}

	protected QueryStepsDag makeQueryStepsDag(ExecutingQueryContext executingQueryContext) {
		IQueryStepsDagBuilder queryStepsDagBuilder = makeQueryStepsDagsBuilder(executingQueryContext);

		// Add explicitly requested steps
		Set<IMeasure> queriedMeasures = getRootMeasures(executingQueryContext);

		queryStepsDagBuilder.registerRootWithUnderlyings(executingQueryContext::resolveIfRef, queriedMeasures);

		return queryStepsDagBuilder.getQueryDag();
	}

	/**
	 * This is especially important to manage the case where no measure is requested, and we have to add some default
	 * measure.
	 * 
	 * @param executingQueryContext
	 * @return the {@link Set} of root measures
	 */
	protected Set<IMeasure> getRootMeasures(ExecutingQueryContext executingQueryContext) {
		Set<IMeasure> measures = executingQueryContext.getQuery().getMeasures();
		Set<IMeasure> queriedMeasures;
		if (measures.isEmpty()) {
			IMeasure defaultMeasure = defaultMeasure();
			queriedMeasures = Set.of(defaultMeasure);
		} else {
			queriedMeasures = measures.stream().peek(m -> {
				if (emptyMeasureName.equals(m.getName())) {
					throw new IllegalArgumentException("The defaultEmptyMeasure can not be requested explicitly");
				}
			}).map(ref -> executingQueryContext.resolveIfRef(ref)).collect(Collectors.toSet());
		}
		return queriedMeasures;
	}

	/**
	 * This measure is used to materialize slices. Typically used to list coordinates along a column.
	 * 
	 * @return the measure to be considered if not measure is provided to the query
	 */
	protected IMeasure defaultMeasure() {
		return Aggregator.builder().name(emptyMeasureName).aggregationKey(EmptyAggregation.KEY).build();
	}

	protected IQueryStepsDagBuilder makeQueryStepsDagsBuilder(ExecutingQueryContext executingQueryContext) {
		return new QueryStepsDagBuilder(operatorsFactory,
				executingQueryContext.getTable().getName(),
				executingQueryContext.getQuery());
	}

	protected ITabularView executeDag(ExecutingQueryContext executingQueryContext, QueryStepsDag queryStepsDag) {
		// bootstrap is a fake empty phase
		// It is relevant to have a log before the `opening` phase which may be slow
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("bootstrap").source(this).build());

		// Execute the leaf aggregations, by tableWrappers
		Map<AdhocQueryStep, ISliceToValue> queryStepToValues =
				executeTableQueries(executingQueryContext, queryStepsDag);

		// We're done with the input stream: the DB can be shutdown, we can answer the
		// rest of the query independently of external tables.
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregate").source(this).build());

		walkDagUpToQueriedMeasures(executingQueryContext, queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transform").source(this).build());

		ITabularView tabularView = toTabularView(executingQueryContext, queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("view").source(this).build());

		return tabularView;
	}

	protected Map<AdhocQueryStep, ISliceToValue> executeTableQueries(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag) {
		AdhocTableQueryEngine makeTableQueryEngine = makeTableQueryEngine();
		return makeTableQueryEngine.executeTableQueries(executingQueryContext, queryStepsDag);
	}

	protected AdhocTableQueryEngine makeTableQueryEngine() {
		return AdhocTableQueryEngine.builder()
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
	protected String simplistic(AdhocQueryStep queryStep) {
		return queryStep.getMeasure().getName();
	}

	/**
	 *
	 * @param queryStep
	 * @return a dense version of the queryStep, for logging purposes
	 */
	protected String dense(AdhocQueryStep queryStep) {
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

	protected void walkDagUpToQueriedMeasures(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag,
			Map<AdhocQueryStep, ISliceToValue> queryStepToValues) {

		try {
			executingQueryContext.getFjp().submit(() -> {
				Stream<AdhocQueryStep> topologicalOrder = queryStepsDag.fromAggregatesToQueried();

				if (executingQueryContext.getOptions().contains(StandardQueryOptions.CONCURRENT)) {
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

					IMeasure measure = executingQueryContext.resolveIfRef(queryStep.getMeasure());

					List<AdhocQueryStep> underlyingSteps = queryStepsDag.underlyingSteps(queryStep);

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

	protected Optional<ISliceToValue> processDagStep(Map<AdhocQueryStep, ISliceToValue> queryStepToValues,
			AdhocQueryStep queryStep,
			List<AdhocQueryStep> underlyingSteps,
			IMeasure measure) {
		if (underlyingSteps.isEmpty()) {
			// This may happen on a Columnator which is missing a required column
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
			ITransformator hasUnderlyingQuerySteps = hasUnderlyingMeasures.wrapNode(operatorsFactory, queryStep);
			ISliceToValue coordinatesToValues;
			try {
				coordinatesToValues = hasUnderlyingQuerySteps.produceOutputColumn(underlyings);
			} catch (RuntimeException e) {
				StringBuilder describeStep = new StringBuilder();

				describeStep.append("Issue computing columns for:").append("\r\n");

				// First, we print only measure as a simplistic shorthand of the step
				describeStep.append("    (measures) m=%s given %s".formatted(simplistic(queryStep),
						underlyingSteps.stream().map(this::simplistic).toList())).append("\r\n");
				// Second, we print the underlying steps as something may be hidden in filters, groupBys, configuration
				describeStep.append("    (steps) step=%s given %s".formatted(dense(queryStep),
						underlyingSteps.stream().map(this::dense).toList())).append("\r\n");

				throw new IllegalStateException(describeStep.toString(), e);
			}

			return Optional.of(coordinatesToValues);
		} else {
			throw new UnsupportedOperationException("%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
		}
	}

	/**
	 * 
	 * @param executingQueryContext
	 * @param queryStepsDag
	 * @param queryStepToValues
	 * @return the output {@link ITabularView}
	 */
	protected ITabularView toTabularView(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag,
			Map<AdhocQueryStep, ISliceToValue> queryStepToValues) {
		if (queryStepToValues.isEmpty()) {
			return MapBasedTabularView.empty();
		}

		// BEWARE some queriedStep may be in the middle of the DAG if it is also the underlying of another step
		Iterator<AdhocQueryStep> stepsToReturn = queryStepsDag.getQueried().iterator();
		long expectedOutputCardinality =
				queryStepToValues.values().stream().mapToLong(ISliceToValue::size).max().getAsLong();

		MapBasedTabularView mapBasedTabularView = MapBasedTabularView.builder()
				// Force a HashMap not to rely on default TreeMap
				.coordinatesToValues(new HashMap<>(Ints.saturatedCast(expectedOutputCardinality)))
				.build();

		stepsToReturn.forEachRemaining(step -> {
			ISliceToValue coordinatesToValues = queryStepToValues.get(step);
			if (coordinatesToValues == null) {
				// Happens on a Columnator missing a required column
			} else {
				boolean isEmptyMeasure = step.getMeasure() instanceof Aggregator agg
						&& EmptyAggregation.isEmpty(agg.getAggregationKey());

				boolean hasCarrierMeasure = executingQueryContext.getOptions()
						.contains(StandardQueryOptions.AGGREGATION_CARRIERS_STAY_WRAPPED)
						&& step.getMeasure() instanceof Aggregator agg
						&& operatorsFactory.makeAggregation(agg) instanceof IAggregationCarrier.IHasCarriers;

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
