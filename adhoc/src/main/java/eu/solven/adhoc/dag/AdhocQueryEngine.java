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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import com.google.common.base.Stopwatch;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.dag.step.AdhocQueryStep;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.data.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.collection.UnionSetAggregator;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.ITransformator;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.util.IAdhocEventBus;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.adhoc.util.IStopwatchFactory;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * The default query-engine.
 *
 * @author Benoit Lacelle
 */
@Builder(toBuilder = true)
@Slf4j
public class AdhocQueryEngine implements IAdhocQueryEngine {
	@NonNull
	@Default
	final IOperatorsFactory operatorsFactory = new StandardOperatorsFactory();

	@NonNull
	@Default
	final IAdhocEventBus eventBus = IAdhocEventBus.BLACK_HOLE;

	@NonNull
	@Default
	IStopwatchFactory stopwatchFactory = () -> {
		Stopwatch stopwatch = Stopwatch.createStarted();

		return stopwatch::elapsed;
	};

	@Override
	public ITabularView execute(ExecutingQueryContext executingQueryContext) {
		IStopwatch stopWatch = stopwatchFactory.createStarted();
		boolean postedAboutDone = false;
		try {
			postAboutQueryDone(executingQueryContext);

			QueryStepsDag queryStepsDag = makeQueryStepsDag(executingQueryContext);

			if (executingQueryContext.isExplain() || executingQueryContext.isDebug()) {
				explainDagSteps(queryStepsDag);
			}

			Set<TableQuery> tableQueries = prepareForTable(executingQueryContext, queryStepsDag);

			Map<TableQuery, ITabularRecordStream> tableQueryToStream = new HashMap<>();
			for (TableQuery tableQuery : tableQueries) {
				ITabularRecordStream rowsStream = openTableStream(executingQueryContext, tableQuery);
				tableQueryToStream.put(tableQuery, rowsStream);
			}

			ITabularView tabularView =
					executeDagGivenRecordStreams(executingQueryContext, queryStepsDag, tableQueryToStream);

			if (executingQueryContext.isExplain() || executingQueryContext.isDebug()) {
				explainDagPerfs(queryStepsDag);
			}

			postAboutQueryDone(executingQueryContext, "OK", stopWatch);
			postedAboutDone = true;
			return tabularView;
		} catch (RuntimeException e) {
			// TODO Add the Exception to the event
			postAboutQueryDone(executingQueryContext, "KO", stopWatch);
			postedAboutDone = true;

			throw new IllegalArgumentException(
					"Issue executing query=%s options=%s".formatted(executingQueryContext.getQuery(),
							executingQueryContext.getOptions()),
					e);
		} finally {
			if (!postedAboutDone) {
				// This may happen in case of OutOfMemoryError, or any uncaught exception
				postAboutQueryDone(executingQueryContext, "KO_Uncaught", stopWatch);
			}
		}
	}

	protected void postAboutQueryDone(ExecutingQueryContext executingQueryContext) {
		eventBus.post(AdhocLogEvent.builder()
				.message("Executing on table=%s measures=%s query=%s".formatted(executingQueryContext.getTable()
						.getName(), executingQueryContext.getMeasures().getName(), executingQueryContext.getQuery()))
				.source(this)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_DONE)
				.build());
	}

	protected void postAboutQueryDone(ExecutingQueryContext executingQueryContext,
			String status,
			IStopwatch stopWatch) {
		eventBus.post(AdhocLogEvent.builder()
				.message("Executed status=%s duration=%s on table=%s measures=%s query=%s".formatted(status,
						stopWatch.elapsed(),
						executingQueryContext.getTable().getName(),
						executingQueryContext.getMeasures().getName(),
						executingQueryContext.getQuery()))
				.source(this)
				.performance(true)
				.tag(AdhocQueryMonitor.TAG_QUERY_LIFECYCLE)
				.tag(AdhocQueryMonitor.TAG_QUERY_DONE)
				.build());
	}

	protected ITabularRecordStream openTableStream(ExecutingQueryContext executingQueryContext, TableQuery tableQuery) {
		IAdhocTableWrapper table = executingQueryContext.getTable();
		return executingQueryContext.getColumnsManager().openTableStream(table, tableQuery);
	}

	protected ITabularView executeDagGivenRecordStreams(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag,
			Map<TableQuery, ITabularRecordStream> tableToRowsStream) {
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("prepare").source(this).build());

		Map<AdhocQueryStep, ISliceToValue> queryStepToValues =
				preAggregate(executingQueryContext, tableToRowsStream, queryStepsDag);

		// We're done with the input stream: the DB can be shutdown, we could answer the
		// query
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregate").source(this).build());

		walkDagUpToQueriedMeasures(executingQueryContext, queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transform").source(this).build());

		ITabularView tabularView = toTabularView(executingQueryContext, queryStepsDag, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("view").source(this).build());

		return tabularView;
	}

	protected Map<AdhocQueryStep, ISliceToValue> preAggregate(ExecutingQueryContext executingQueryContext,
			Map<TableQuery, ITabularRecordStream> tableToRowsStream,
			QueryStepsDag queryStepsDag) {
		SetMultimap<String, Aggregator> columnToAggregators = columnToAggregators(executingQueryContext, queryStepsDag);

		Map<AdhocQueryStep, ISliceToValue> queryStepToValues = new LinkedHashMap<>();

		// This is the only step consuming the input stream
		tableToRowsStream.forEach((tableQuery, rowsStream) -> {
			IStopwatch stopWatch = stopwatchFactory.createStarted();

			Map<AdhocQueryStep, ISliceToValue> oneQueryStepToValues =
					aggregateStreamToAggregates(executingQueryContext, tableQuery, rowsStream, columnToAggregators);

			Duration elapsed = stopWatch.elapsed();

			tableQuery.getAggregators().forEach(aggregator -> {
				AdhocQueryStep queryStep = AdhocQueryStep.edit(tableQuery).measure(aggregator).build();

				ISliceToValue column = oneQueryStepToValues.get(queryStep);

				eventBus.post(QueryStepIsCompleted.builder()
						.querystep(queryStep)
						.nbCells(column.size())
						// The duration is not decomposed per aggregator
						.duration(elapsed)
						.source(AdhocQueryEngine.this)
						.build());

				queryStepsDag.registerExecutionFeedback(queryStep,
						SizeAndDuration.builder().size(column.size()).duration(elapsed).build());
			});

			queryStepToValues.putAll(oneQueryStepToValues);
		});

		if (executingQueryContext.isDebug()) {
			queryStepToValues.forEach((aggregateStep, values) -> {
				values.forEachSlice(row -> {
					return rowValue -> {
						eventBus.post(AdhocLogEvent.builder()
								.debug(true)
								.message("%s -> %s".formatted(rowValue, row))
								.source(aggregateStep)
								.build());
					};
				});

			});
		}
		return queryStepToValues;
	}

	protected SetMultimap<String, Aggregator> columnToAggregators(ExecutingQueryContext executingQueryContext,
			QueryStepsDag dagHolder) {
		SetMultimap<String, Aggregator> columnToAggregators =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = dagHolder.getDag();
		dag.vertexSet()
				.stream()
				.filter(step -> dag.outDegreeOf(step) == 0)
				.map(AdhocQueryStep::getMeasure)
				.forEach(measure -> {
					measure = executingQueryContext.resolveIfRef(measure);

					if (measure instanceof Aggregator a) {
						columnToAggregators.put(a.getColumnName(), a);
					} else if (measure instanceof EmptyMeasure) {
						// ???
					} else if (measure instanceof Columnator) {
						// ???
						// Happens if we miss given column
					} else {
						throw new UnsupportedOperationException(
								"%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
					}
				});
		return columnToAggregators;
	}

	protected ITabularView toTabularView(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag,
			Map<AdhocQueryStep, ISliceToValue> queryStepToValues) {
		if (queryStepToValues.isEmpty()) {
			return MapBasedTabularView.empty();
		}

		long expectedOutputCardinality;
		Iterator<AdhocQueryStep> stepsToReturn;
		if (executingQueryContext.getOptions().contains(StandardQueryOptions.RETURN_UNDERLYING_MEASURES)) {
			// BEWARE Should we return steps with same groupBy?
			// BEWARE This does not work if there is multiple steps on same measure, as we later groupBy measureName
			// What about measures appearing multiple times in the DAG?
			stepsToReturn = new BreadthFirstIterator<>(queryStepsDag.getDag());
			expectedOutputCardinality = 0;
		} else {
			// BEWARE some queriedStep may be in the middle of the DAG if it is also the underlying of another step
			stepsToReturn = queryStepsDag.getQueried().iterator();
			expectedOutputCardinality =
					queryStepToValues.values().stream().mapToLong(ISliceToValue::size).max().getAsLong();
		}

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
				} else {
					rowScanner = baseRowScanner;
				}
				coordinatesToValues.forEachSlice(rowScanner);
			}
		});
		return mapBasedTabularView;
	}

	protected Map<AdhocQueryStep, ISliceToValue> aggregateStreamToAggregates(
			ExecutingQueryContext executingQueryContext,
			TableQuery query,
			ITabularRecordStream stream,
			SetMultimap<String, Aggregator> columnToAggregators) {

		IMultitypeMergeableGrid<SliceAsMap> coordinatesToAggregates =
				sinkToAggregates(executingQueryContext, query, stream, columnToAggregators);

		return toImmutableChunks(query, coordinatesToAggregates);
	}

	protected Map<AdhocQueryStep, ISliceToValue> toImmutableChunks(TableQuery tableQuery,
			IMultitypeMergeableGrid<SliceAsMap> coordinatesToAggregates) {
		Map<AdhocQueryStep, ISliceToValue> queryStepToValues = new HashMap<>();
		tableQuery.getAggregators().forEach(aggregator -> {
			AdhocQueryStep queryStep = AdhocQueryStep.edit(tableQuery).measure(aggregator).build();

			// `.closeColumn` is an expensive operation. It induces a delay, e.g. by sorting slices.
			IMultitypeColumnFastGet<SliceAsMap> column = coordinatesToAggregates.closeColumn(aggregator);

			// eventBus.post(QueryStepIsCompleted.builder()
			// .querystep(queryStep)
			// .nbCells(column.size())
			// // TODO How to collect this duration? Especially as the tableQuery computes multiple aggregators at
			// // the same time
			// .duration(Duration.ZERO)
			// .source(this)
			// .build());
			// log.debug("tableQuery={} generated a column with size={}", tableQuery, column.size());

			// The aggregation step is done: the storage is supposed not to be edited: we
			// re-use it in place, to spare a copy to an immutable container
			queryStepToValues.put(queryStep, SliceToValue.builder().column(column).build());
		});
		return queryStepToValues;
	}

	protected void walkDagUpToQueriedMeasures(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag,
			Map<AdhocQueryStep, ISliceToValue> queryStepToValues) {
		queryStepsDag.fromAggregatesToQueried().forEachRemaining(queryStep -> {

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
					throw new IllegalStateException("The DAG missed values for step=%s".formatted(underlyingStep));
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

	protected IMultitypeMergeableGrid<SliceAsMap> sinkToAggregates(ExecutingQueryContext executingQueryContext,
			TableQuery tableQuery,
			ITabularRecordStream stream,
			SetMultimap<String, Aggregator> columnToAggregators) {

		IAggregatedRecordStreamReducer streamReducer =
				makeAggregatedRecordStreamReducer(executingQueryContext, tableQuery, columnToAggregators);

		return streamReducer.reduce(stream);

	}

	protected IAggregatedRecordStreamReducer makeAggregatedRecordStreamReducer(
			ExecutingQueryContext executingQueryContext,
			TableQuery tableQuery,
			SetMultimap<String, Aggregator> columnToAggregators) {
		return AggregatedRecordStreamReducer.builder()
				.operatorsFactory(operatorsFactory)
				.executingQueryContext(executingQueryContext)
				.tableQuery(tableQuery)
				.columnToAggregators(columnToAggregators)
				.build();
	}

	protected Optional<Aggregator> isAggregator(Map<String, Set<Aggregator>> columnToAggregators,
			String aggregatorName) {
		return columnToAggregators.values()
				.stream()
				.flatMap(Collection::stream)
				.filter(a -> a.getName().equals(aggregatorName))
				.findAny();
	}

	/**
	 * @param executingQueryContext
	 * @param dagHolder2
	 * @return the Set of {@link TableQuery} to be executed.
	 */
	protected Set<TableQuery> prepareForTable(ExecutingQueryContext executingQueryContext,
			QueryStepsDag queryStepsDag) {
		// Pack each steps targeting the same groupBy+filters. Multiple measures can be evaluated on such packs.
		Map<MeasurelessQuery, Set<Aggregator>> measurelessToAggregators = new HashMap<>();

		// https://stackoverflow.com/questions/57134161/how-to-find-roots-and-leaves-set-in-jgrapht-directedacyclicgraph
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = queryStepsDag.getDag();
		dag.vertexSet().stream().filter(step -> dag.outgoingEdgesOf(step).isEmpty()).forEach(step -> {
			IMeasure leafMeasure = executingQueryContext.resolveIfRef(step.getMeasure());

			if (leafMeasure instanceof Aggregator leafAggregator) {
				MeasurelessQuery measureless = MeasurelessQuery.edit(step).build();

				// We could analyze filters, to discard a query filter `k=v` if another query
				// filters `k=v|v2`
				measurelessToAggregators
						.merge(measureless, Collections.singleton(leafAggregator), UnionSetAggregator::unionSet);
			} else if (leafMeasure instanceof EmptyMeasure) {
				// ???
			} else if (leafMeasure instanceof Columnator) {
				// ???
				// Happens if we miss given column
			} else {
				throw new IllegalStateException("Expected simple aggregators. Got %s".formatted(leafMeasure));
			}
		});

		return measurelessToAggregators.entrySet().stream().map(e -> {
			MeasurelessQuery measurelessQuery = e.getKey();
			Set<Aggregator> leafAggregators = e.getValue();
			return TableQuery.edit(measurelessQuery).aggregators(leafAggregators).build();
		}).collect(Collectors.toSet());
	}

	protected void explainDagSteps(QueryStepsDag queryStepsDag) {
		makeDagExplainer().explain(queryStepsDag);
	}

	protected void explainDagPerfs(QueryStepsDag queryStepsDag) {
		makeDagExplainerForPerfs().explain(queryStepsDag);
	}

	protected DagExplainer makeDagExplainer() {
		return DagExplainer.builder().eventBus(eventBus).build();
	}

	protected DagExplainerForPerfs makeDagExplainerForPerfs() {
		return DagExplainerForPerfs.builder().eventBus(eventBus).build();
	}

	protected QueryStepsDag makeQueryStepsDag(ExecutingQueryContext executingQueryContext) {
		QueryStepsDagBuilder queryStepsDagBuilder = makeQueryStepsDagsBuilder(executingQueryContext);

		// Add explicitly requested steps
		Set<IMeasure> queriedMeasures = convertToQueriedSteps(executingQueryContext);
		queriedMeasures.forEach(queryStepsDagBuilder::addRoot);

		// Add implicitly requested steps
		while (queryStepsDagBuilder.hasLeftovers()) {
			AdhocQueryStep parentStep = queryStepsDagBuilder.pollLeftover();

			IMeasure measure = executingQueryContext.resolveIfRef(parentStep.getMeasure());

			if (measure instanceof Aggregator aggregator) {
				log.debug("Aggregators (here {}) do not have any underlying measure", aggregator);
			} else if (measure instanceof IHasUnderlyingMeasures measureWithUnderlyings) {
				ITransformator wrappedQueryStep = measureWithUnderlyings.wrapNode(operatorsFactory, parentStep);

				List<AdhocQueryStep> underlyingSteps =
						wrappedQueryStep.getUnderlyingSteps().stream().map(underlyingStep -> {
							// Make sure the DAG has actual measure nodes, and not references
							IMeasure notRefMeasure = executingQueryContext.resolveIfRef(underlyingStep.getMeasure());
							return AdhocQueryStep.edit(underlyingStep).measure(notRefMeasure).build();
						}).toList();

				queryStepsDagBuilder.registerUnderlyings(parentStep, underlyingSteps);
			} else {
				throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(measure).toString());
			}
		}

		queryStepsDagBuilder.sanityChecks();

		return queryStepsDagBuilder.getQueryDag();
	}

	protected Set<IMeasure> convertToQueriedSteps(ExecutingQueryContext executingQueryContext) {
		Set<IMeasure> measures = executingQueryContext.getQuery().getMeasures();
		Set<IMeasure> queriedMeasures;
		if (measures.isEmpty()) {
			IMeasure defaultMeasure = defaultMeasure();
			queriedMeasures = Set.of(defaultMeasure);
		} else {
			queriedMeasures =
					measures.stream().map(ref -> executingQueryContext.resolveIfRef(ref)).collect(Collectors.toSet());
		}
		return queriedMeasures;
	}

	/**
	 * This measure is used to materialize slices. Typically used to list coordinates along a column.
	 * 
	 * @return the measure to be considered if not measure is provided to the query
	 */
	protected IMeasure defaultMeasure() {
		return Aggregator.builder().name("empty").aggregationKey(EmptyAggregation.class.getName()).build();
	}

	protected QueryStepsDagBuilder makeQueryStepsDagsBuilder(ExecutingQueryContext executingQueryContext) {
		return new QueryStepsDagBuilder(executingQueryContext.getTable().getName(), executingQueryContext.getQuery());
	}

}
