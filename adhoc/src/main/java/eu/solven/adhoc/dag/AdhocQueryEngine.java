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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;

import com.google.common.primitives.Ints;

import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.collection.UnionSetAggregator;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.ITransformator;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.cube.IHasGroupBy;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.record.IAggregatedRecord;
import eu.solven.adhoc.record.IAggregatedRecordStream;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.storage.AggregatingMeasurators;
import eu.solven.adhoc.storage.IMergeableMultitypeColumn;
import eu.solven.adhoc.storage.IRowScanner;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import eu.solven.adhoc.storage.MergeableMultiTypeStorage;
import eu.solven.adhoc.storage.SliceToValue;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.util.IAdhocEventBus;
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
@Builder
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
	final IAdhocImplicitFilter implicitFilter = query -> IAdhocFilter.MATCH_ALL;

	@Override
	public ITabularView execute(ExecutingQueryContext rawExecutingQueryContext) {
		ExecutingQueryContext executingQueryContext = preprocessQuery(rawExecutingQueryContext);

		boolean postedAboutDone = false;
		try {
			eventBus.post(AdhocLogEvent.builder()
					.message("Executing on table=%s measures=%s query=%s".formatted(
							executingQueryContext.getTable().getName(),
							executingQueryContext.getMeasures().getName(),
							executingQueryContext.getQuery()))
					.source(this)
					.build());

			Set<TableQuery> tableQueries = prepareForTable(executingQueryContext);

			Map<TableQuery, IAggregatedRecordStream> tableQueryToStream = new HashMap<>();
			for (TableQuery tableQuery : tableQueries) {
				IAggregatedRecordStream rowsStream = openTableStream(executingQueryContext, tableQuery);
				tableQueryToStream.put(tableQuery, rowsStream);
			}

			ITabularView tabularView = execute(executingQueryContext, tableQueryToStream);

			postAboutQueryDone(executingQueryContext, "OK");
			postedAboutDone = true;
			return tabularView;
		} catch (RuntimeException e) {
			// TODO Add the Exception to the event
			postAboutQueryDone(executingQueryContext, "KO");
			postedAboutDone = true;

			throw new IllegalArgumentException(
					"Issue executing query=%s options=%s".formatted(executingQueryContext.getQuery(),
							executingQueryContext.getOptions()),
					e);
		} finally {
			if (!postedAboutDone) {
				// This may happen in case of OutOfMemoryError, or any uncaught exception
				postAboutQueryDone(executingQueryContext, "KO_Uncaught");
			}
		}
	}

	protected ExecutingQueryContext preprocessQuery(ExecutingQueryContext rawExecutingQueryContext) {
		IAdhocQuery query = rawExecutingQueryContext.getQuery();
		IAdhocFilter preprocessedFilter = AndFilter.and(query.getFilter(), implicitFilter.getImplicitFilter(query));

		AdhocQuery preprocessedQuery = AdhocQuery.edit(query).filter(preprocessedFilter).build();
		return rawExecutingQueryContext.toBuilder().query(preprocessedQuery).build();
	}

	private void postAboutQueryDone(ExecutingQueryContext executingQueryContext, String status) {
		eventBus.post(AdhocLogEvent.builder()
				.message("Executed status=%s on table=%s measures=%s query=%s".formatted(status,
						executingQueryContext.getTable().getName(),
						executingQueryContext.getMeasures().getName(),
						executingQueryContext.getQuery()))
				.source(this)
				.build());
	}

	protected IAggregatedRecordStream openTableStream(ExecutingQueryContext executingQueryContext,
			TableQuery tableQuery) {
		IAdhocTableWrapper table = executingQueryContext.getTable();
		return executingQueryContext.getColumnsManager().openTableStream(table, tableQuery);
	}

	protected ITabularView execute(ExecutingQueryContext executingQueryContext,
			Map<TableQuery, IAggregatedRecordStream> tableToRowsStream) {
		DagHolder fromQueriedToAggregates = makeQueryStepsDag(executingQueryContext);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("prepare").source(this).build());

		Map<AdhocQueryStep, ISliceToValue> queryStepToValues =
				preAggregate(executingQueryContext, tableToRowsStream, fromQueriedToAggregates);

		// We're done with the input stream: the DB can be shutdown, we could answer the
		// query
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregate").source(this).build());

		walkDagUpToQueriedMeasures(executingQueryContext, fromQueriedToAggregates, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transform").source(this).build());

		ITabularView tabularView = toTabularView(executingQueryContext, fromQueriedToAggregates, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("view").source(this).build());

		return tabularView;
	}

	protected Map<AdhocQueryStep, ISliceToValue> preAggregate(ExecutingQueryContext executingQueryContext,
			Map<TableQuery, IAggregatedRecordStream> tableToRowsStream,
			DagHolder fromQueriedToAggregates) {
		Map<String, Set<Aggregator>> columnToAggregators =
				columnToAggregators(executingQueryContext, fromQueriedToAggregates);

		Map<AdhocQueryStep, ISliceToValue> queryStepToValues = new LinkedHashMap<>();

		// This is the only step consuming the input stream
		tableToRowsStream.forEach((tableQuery, rowsStream) -> {
			Map<AdhocQueryStep, SliceToValue> oneQueryStepToValues =
					aggregateStreamToAggregates(executingQueryContext, tableQuery, rowsStream, columnToAggregators);

			queryStepToValues.putAll(oneQueryStepToValues);
		});

		if (executingQueryContext.isDebug()) {
			queryStepToValues.forEach((aggregateStep, values) -> {
				values.forEachSlice(row -> {
					return o -> {
						eventBus.post(AdhocLogEvent.builder()
								.debug(true)
								.message("%s -> %s step={}".formatted(o, row))
								.source(aggregateStep));
					};
				});

			});
		}
		return queryStepToValues;
	}

	protected Map<String, Set<Aggregator>> columnToAggregators(ExecutingQueryContext executingQueryContext,
			DagHolder dagHolder) {
		Map<String, Set<Aggregator>> columnToAggregators = new LinkedHashMap<>();

		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = dagHolder.getDag();
		dag.vertexSet()
				.stream()
				.filter(step -> dag.outDegreeOf(step) == 0)
				.map(AdhocQueryStep::getMeasure)
				.forEach(measure -> {
					measure = executingQueryContext.resolveIfRef(measure);

					if (measure instanceof Aggregator a) {
						columnToAggregators.merge(a.getColumnName(), Set.of(a), UnionSetAggregator::unionSet);
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
			DagHolder fromQueriedToAggregates,
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
			stepsToReturn = new BreadthFirstIterator<>(fromQueriedToAggregates.getDag());
			expectedOutputCardinality = 0;
		} else {
			// BEWARE some queriedStep may be in the middle of the DAG if it is also the underlying of another step
			stepsToReturn = fromQueriedToAggregates.getQueried().iterator();
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
				IRowScanner<AdhocSliceAsMap> rowScanner = slice -> {
					return mapBasedTabularView.sliceFeeder(slice, step.getMeasure().getName());
				};

				coordinatesToValues.forEachSlice(rowScanner);
			}
		});
		return mapBasedTabularView;
	}

	protected Map<AdhocQueryStep, SliceToValue> aggregateStreamToAggregates(ExecutingQueryContext executingQueryContext,
			TableQuery query,
			IAggregatedRecordStream stream,
			Map<String, Set<Aggregator>> columnToAggregators) {

		AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAggregates =
				sinkToAggregates(executingQueryContext, query, stream, columnToAggregators);

		return toImmutableChunks(query, coordinatesToAggregates);
	}

	protected Map<AdhocQueryStep, SliceToValue> toImmutableChunks(TableQuery tableQuery,
			AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAggregates) {
		Map<AdhocQueryStep, SliceToValue> queryStepToValues = new HashMap<>();
		tableQuery.getAggregators().forEach(aggregator -> {
			AdhocQueryStep queryStep = AdhocQueryStep.edit(tableQuery).measure(aggregator).build();

			IMergeableMultitypeColumn<AdhocSliceAsMap> storage =
					coordinatesToAggregates.getAggregatorToStorage().get(aggregator);

			if (storage == null) {
				// Typically happens when a filter reject completely one of the underlying
				// measure
				storage = MergeableMultiTypeStorage.empty();
			} else {
				// Typically converts a CountHolder into the count as a `long`
				storage.purgeAggregationCarriers();
			}

			eventBus.post(
					QueryStepIsCompleted.builder().querystep(queryStep).nbCells(storage.size()).source(this).build());
			log.debug("tableQuery={} generated a column with size={}", tableQuery, storage.size());

			// The aggregation step is done: the storage is supposed not to be edited: we
			// re-use it in place, to
			// spare a copy to an immutable container
			queryStepToValues.put(queryStep, SliceToValue.builder().storage(storage).build());
		});
		return queryStepToValues;
	}

	protected void walkDagUpToQueriedMeasures(ExecutingQueryContext executingQueryContext,
			DagHolder fromQueriedToAggregates,
			Map<AdhocQueryStep, ISliceToValue> queryStepToValues) {
		// https://stackoverflow.com/questions/69183360/traversal-of-edgereversedgraph
		EdgeReversedGraph<AdhocQueryStep, DefaultEdge> fromAggregatesToQueried =
				new EdgeReversedGraph<>(fromQueriedToAggregates.getDag());

		// https://en.wikipedia.org/wiki/Topological_sorting
		// TopologicalOrder guarantees processing a vertex after dependent vertices are
		// done.
		Iterator<AdhocQueryStep> graphIterator = new TopologicalOrderIterator<>(fromAggregatesToQueried);

		graphIterator.forEachRemaining(queryStep -> {

			if (queryStepToValues.containsKey(queryStep)) {
				// This typically happens on aggregator measures, as they are fed in a previous
				// step. Here, we want to process a measure once its underlying steps are completed
				return;
			} else if (queryStep.getMeasure() instanceof Aggregator a) {
				throw new IllegalStateException("Missing values for %s".formatted(a));
			}

			eventBus.post(QueryStepIsEvaluating.builder().queryStep(queryStep).source(this).build());

			IMeasure measure = executingQueryContext.resolveIfRef(queryStep.getMeasure());

			Set<DefaultEdge> underlyingStepEdges = fromAggregatesToQueried.incomingEdgesOf(queryStep);
			List<AdhocQueryStep> underlyingSteps = underlyingStepEdges.stream()
					.map(edge -> Graphs.getOppositeVertex(fromAggregatesToQueried, edge, queryStep))
					.toList();

			processDagStep(queryStepToValues, queryStep, underlyingSteps, measure);
		});
	}

	private void processDagStep(Map<AdhocQueryStep, ISliceToValue> queryStepToValues,
			AdhocQueryStep queryStep,
			List<AdhocQueryStep> underlyingSteps,
			IMeasure measure) {
		if (underlyingSteps.isEmpty()) {
			// This may happen on a Columnator which is missing a required column
			return;
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

				describeStep.append("Issue computing columns for:");

				// First, we print only measure as a simplistic shorthand of the step
				describeStep.append("    (measures) m=%s given %s".formatted(simplistic(queryStep),
						underlyingSteps.stream().map(this::simplistic).toList())).append("\r\n");
				// Second, we print the underlying steps as something may be hidden in filters, groupBys, configuration
				describeStep.append("    (steps) step=%s given %s".formatted(dense(queryStep),
						underlyingSteps.stream().map(this::dense).toList())).append("\r\n");

				throw new IllegalStateException(describeStep.toString(), e);
			}

			eventBus.post(QueryStepIsCompleted.builder()
					.querystep(queryStep)
					.nbCells(coordinatesToValues.size())
					.source(this)
					.build());

			queryStepToValues.put(queryStep, coordinatesToValues);
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
				.append("filter=")
				.append(queryStep.getFilter())
				.append("groupBy=")
				.append(queryStep.getGroupBy())
				.append("custom=")
				.append(queryStep.getCustomMarker())
				.toString();
	}

	protected AggregatingMeasurators<AdhocSliceAsMap> sinkToAggregates(ExecutingQueryContext executingQueryContext,
			TableQuery tableQuery,
			IAggregatedRecordStream stream,
			Map<String, Set<Aggregator>> columnToAggregators) {

		AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAgg = makeAggregatingMeasures();

		Set<String> relevantColumns = new HashSet<>();
		// We may receive raw columns,to be aggregated by ourselves
		relevantColumns.addAll(columnToAggregators.keySet());
		// We may also receive pre-aggregated columns
		columnToAggregators.values()
				.stream()
				.flatMap(Collection::stream)
				.map(Aggregator::getName)
				.forEach(relevantColumns::add);

		// TODO We'd like to log on the last row, to have the number if row actually
		// streamed
		BiConsumer<IAggregatedRecord, Optional<AdhocSliceAsMap>> peekOnCoordinate = prepareStreamLogger(tableQuery);

		// Process the underlying stream of data to execute aggregations
		try {
			stream.asMap().forEach(input -> {
				forEachStreamedRow(executingQueryContext,
						tableQuery,
						columnToAggregators,
						input,
						peekOnCoordinate,
						relevantColumns,
						coordinatesToAgg);
			});
		} catch (RuntimeException e) {
			throw new RuntimeException("Issue processing stream from %s".formatted(stream), e);
		}

		return coordinatesToAgg;

	}

	private AggregatingMeasurators<AdhocSliceAsMap> makeAggregatingMeasures() {
		return new AggregatingMeasurators<>(operatorsFactory);
	}

	protected BiConsumer<IAggregatedRecord, Optional<AdhocSliceAsMap>> prepareStreamLogger(TableQuery tableQuery) {
		AtomicInteger nbIn = new AtomicInteger();
		AtomicInteger nbOut = new AtomicInteger();

		BiConsumer<IAggregatedRecord, Optional<AdhocSliceAsMap>> peekOnCoordinate = (input, optCoordinates) -> {

			if (optCoordinates.isEmpty()) {
				// Skip this input as it is incompatible with the groupBy
				// This may not be done by IAdhocDatabaseWrapper for complex groupBys.
				// TODO Wouldn't this be a bug in IAdhocDatabaseWrapper?
				int currentOut = nbOut.incrementAndGet();
				if (tableQuery.isDebug() && Integer.bitCount(currentOut) == 1) {
					log.info("Rejected row #{}: {}", currentOut, input);
				}
			} else {
				int currentIn = nbIn.incrementAndGet();
				if (tableQuery.isDebug() && Integer.bitCount(currentIn) == 1) {
					log.info("Accepted row #{}: {}", currentIn, input);
				}
			}
		};
		return peekOnCoordinate;
	}

	protected void forEachStreamedRow(ExecutingQueryContext executingQueryContext,
			IHasGroupBy tableQuery,
			Map<String, Set<Aggregator>> columnToAggregators,
			IAggregatedRecord tableRow,
			BiConsumer<IAggregatedRecord, Optional<AdhocSliceAsMap>> peekOnCoordinate,
			Set<String> aggregatorColumns,
			AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAgg) {
		Optional<AdhocSliceAsMap> optCoordinates = makeCoordinate(executingQueryContext, tableQuery, tableRow);

		peekOnCoordinate.accept(tableRow, optCoordinates);

		if (optCoordinates.isEmpty()) {
			return;
		}

		AdhocSliceAsMap coordinates = optCoordinates.get();

		for (String aggregatedColumn : aggregatorColumns) {
			Object v = tableRow.getAggregate(aggregatedColumn);
			if (v != null) {
				Set<Aggregator> aggs = columnToAggregators.get(aggregatedColumn);

				if (aggs == null) {
					// DB has done the aggregation for us
					Optional<Aggregator> optAgg = isAggregator(columnToAggregators, aggregatedColumn);

					optAgg.ifPresent(agg -> {
						coordinatesToAgg.contributeAggregate(agg, coordinates, v);
					});
				} else {
					// The DB provides the column raw value, and not an aggregated value
					// So we aggregate row values ourselves (e.g. InMemoryTable)
					aggs.forEach(agg -> coordinatesToAgg.contributeRaw(agg, coordinates, v));
				}
			}
		}
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
	 * @param tableQuery
	 * @param tableRow
	 * @return the coordinate for given input, or empty if the input is not compatible with given groupBys.
	 */
	protected Optional<AdhocSliceAsMap> makeCoordinate(ExecutingQueryContext executingQueryContext,
			IHasGroupBy tableQuery,
			IAggregatedRecord tableRow) {
		IAdhocGroupBy groupBy = tableQuery.getGroupBy();
		if (groupBy.isGrandTotal()) {
			return Optional.of(AdhocSliceAsMap.fromMap(Collections.emptyMap()));
		}

		NavigableSet<String> groupedByColumns = groupBy.getGroupedByColumns();

		AdhocMap.AdhocMapBuilder coordinatesBuilder = AdhocMap.builder(groupedByColumns);

		for (String groupedByColumn : groupedByColumns) {
			Object value = tableRow.getGroupBy(groupedByColumn);

			if (value == null) {
				if (tableRow.getGroupBys().containsKey(groupedByColumn)) {
					// We received an explicit null
					// Typically happens on a failed LEFT JOIN
					value = valueOnNull(executingQueryContext, groupedByColumn);

					assert value != null : "`null` is not a legal column value";
				} else {
					// The input lack a groupBy coordinate: we exclude it
					// TODO What's a legitimate case leading to this?
					return Optional.empty();
				}
			}

			coordinatesBuilder.append(value);
		}

		return Optional.of(AdhocSliceAsMap.fromMap(coordinatesBuilder.build()));
	}

	/**
	 * The value to inject in place of a NULL. Returning a null-reference is not supported.
	 *
	 * @param column
	 *            the column over which a null is encountered. You may customize `null` behavior on a per-column basis.
	 */
	protected Object valueOnNull(ExecutingQueryContext executingQueryContext, String column) {
		return executingQueryContext.getColumnsManager().onMissingColumn(column);
	}

	/**
	 * @param executingQueryContext
	 * @return the Set of {@link TableQuery} to be executed.
	 */
	public Set<TableQuery> prepareForTable(ExecutingQueryContext executingQueryContext) {
		DagHolder dagHolder = makeQueryStepsDag(executingQueryContext);

		if (executingQueryContext.isExplain() || executingQueryContext.isDebug()) {
			explainDagSteps(dagHolder);
		}

		return queryStepsDagToTableQueries(executingQueryContext, dagHolder);

	}

	protected Set<TableQuery> queryStepsDagToTableQueries(ExecutingQueryContext executingQueryContext,
			DagHolder dagHolder) {
		// Pack each steps targeting the same groupBy+filters. Multiple measures can be evaluated on such packs.
		Map<MeasurelessQuery, Set<Aggregator>> measurelessToAggregators = new HashMap<>();

		// https://stackoverflow.com/questions/57134161/how-to-find-roots-and-leaves-set-in-jgrapht-directedacyclicgraph
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = dagHolder.getDag();
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

	protected void explainDagSteps(DagHolder dag) {
		makeDagExplainer().explain(dag);
	}

	protected DagExplainer makeDagExplainer() {
		return DagExplainer.builder().eventBus(eventBus).build();
	}

	protected DagHolder makeQueryStepsDag(ExecutingQueryContext executingQueryContext) {
		QueryStepsDagsBuilder queryStepsDagBuilder = makeQueryStepsDagsBuilder(executingQueryContext);

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
				for (AdhocQueryStep underlyingStep : wrappedQueryStep.getUnderlyingSteps()) {
					// Make sure the DAG has actual measure nodes, and not references
					IMeasure notRefMeasure = executingQueryContext.resolveIfRef(underlyingStep.getMeasure());
					underlyingStep = AdhocQueryStep.edit(underlyingStep).measure(notRefMeasure).build();

					queryStepsDagBuilder.addEdge(parentStep, underlyingStep);
				}
			} else {
				throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(measure).toString());
			}
		}

		queryStepsDagBuilder.sanityChecks();

		return queryStepsDagBuilder.getQueryDag();
	}

	private Set<IMeasure> convertToQueriedSteps(ExecutingQueryContext executingQueryContext) {
		Set<ReferencedMeasure> measureRefs = executingQueryContext.getQuery().getMeasureRefs();
		Set<IMeasure> queriedMeasures;
		if (measureRefs.isEmpty()) {
			IMeasure defaultMeasure = defaultMeasure();
			queriedMeasures = Set.of(defaultMeasure);
		} else {
			queriedMeasures = measureRefs.stream()
					.map(ref -> executingQueryContext.resolveIfRef(ref))
					.collect(Collectors.toSet());
		}
		return queriedMeasures;
	}

	// Not a single measure is selected: we are doing a DISTINCT query

	/**
	 * This measure is used to materialize slices.
	 * 
	 * @return the measure to be considered if not measure is provided to the query
	 */
	protected IMeasure defaultMeasure() {
		return Aggregator.countAsterisk();
		// return
		// Aggregator.builder().name("CONSTANT_1").aggregationKey("COUNT").columnName("*").build();
	}

	protected QueryStepsDagsBuilder makeQueryStepsDagsBuilder(ExecutingQueryContext executingQueryContext) {
		return new QueryStepsDagsBuilder(executingQueryContext.getTable().getName(), executingQueryContext.getQuery());
	}

	public static AdhocQueryEngineBuilder edit(AdhocQueryEngine engine) {
		return AdhocQueryEngine.builder().operatorsFactory(engine.operatorsFactory).eventBus(engine.eventBus);
	}
}
