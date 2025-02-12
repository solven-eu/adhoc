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

import eu.solven.adhoc.column.DefaultMissingColumnManager;
import eu.solven.adhoc.column.IMissingColumnManager;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.measure.EmptyMeasure;
import eu.solven.adhoc.measure.IMeasure;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.collection.UnionSetAggregator;
import eu.solven.adhoc.measure.step.Aggregator;
import eu.solven.adhoc.measure.step.Columnator;
import eu.solven.adhoc.measure.step.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.step.IHasUnderlyingQuerySteps;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.cube.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.storage.AggregatingMeasurators;
import eu.solven.adhoc.storage.IRowScanner;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import eu.solven.adhoc.storage.MultiTypeStorage;
import eu.solven.adhoc.storage.SliceToValue;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.table.IRowsStream;
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
	final IMissingColumnManager missingColumnManager = new DefaultMissingColumnManager();

	@NonNull
	final IAdhocEventBus eventBus;

	@Override
	public ITabularView execute(AdhocExecutingQueryContext queryWithContext, IAdhocTableWrapper table) {
		Set<TableQuery> prepared = prepareForTable(queryWithContext);

		Map<TableQuery, IRowsStream> dbQueryToStream = new HashMap<>();
		for (TableQuery dbQuery : prepared) {
			dbQueryToStream.put(dbQuery, table.openDbStream(dbQuery));
		}

		return execute(queryWithContext, dbQueryToStream);
	}

	protected ITabularView execute(AdhocExecutingQueryContext queryWithContext,
			Map<TableQuery, IRowsStream> dbQueryToStream) {
		DagHolder fromQueriedToAggregates = makeQueryStepsDag(queryWithContext);

		Map<String, Set<Aggregator>> inputColumnToAggregators =
				columnToAggregators(queryWithContext, fromQueriedToAggregates);

		Map<AdhocQueryStep, ISliceToValue> queryStepToValues = new LinkedHashMap<>();

		// This is the only step consuming the input stream
		dbQueryToStream.forEach((dbQuery, stream) -> {
			Map<AdhocQueryStep, SliceToValue> oneQueryStepToValues =
					aggregateStreamToAggregates(dbQuery, stream, inputColumnToAggregators);

			queryStepToValues.putAll(oneQueryStepToValues);
		});

		if (queryWithContext.isDebug()) {
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

		// We're done with the input stream: the DB can be shutdown, we could answer the
		// query
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregates").source(this).build());

		walkDagUpToQueriedMeasures(queryWithContext, fromQueriedToAggregates, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transformations").source(this).build());

		ITabularView mapBasedTabularView = toTabularView(queryWithContext, fromQueriedToAggregates, queryStepToValues);

		return mapBasedTabularView;
	}

	protected Map<String, Set<Aggregator>> columnToAggregators(AdhocExecutingQueryContext queryWithContext,
			DagHolder dagHolder) {
		Map<String, Set<Aggregator>> columnToAggregators = new LinkedHashMap<>();

		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = dagHolder.getDag();
		dag.vertexSet()
				.stream()
				.filter(step -> dag.outDegreeOf(step) == 0)
				.map(AdhocQueryStep::getMeasure)
				.forEach(measure -> {
					measure = queryWithContext.resolveIfRef(measure);

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

	protected ITabularView toTabularView(AdhocExecutingQueryContext queryWithContext,
			DagHolder fromQueriedToAggregates,
			Map<AdhocQueryStep, ISliceToValue> queryStepToValues) {
		if (queryStepToValues.isEmpty()) {
			return MapBasedTabularView.empty();
		}

		long expectedOutputCardinality;
		Iterator<AdhocQueryStep> stepsToReturn;
		if (queryWithContext.getOptions().contains(StandardQueryOptions.RETURN_UNDERLYING_MEASURES)) {
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

	protected Map<AdhocQueryStep, SliceToValue> aggregateStreamToAggregates(TableQuery dbQuery,
			IRowsStream stream,
			Map<String, Set<Aggregator>> columnToAggregators) {

		AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAggregates =
				sinkToAggregates(dbQuery, stream, columnToAggregators);

		return toImmutableChunks(dbQuery, coordinatesToAggregates);
	}

	protected Map<AdhocQueryStep, SliceToValue> toImmutableChunks(TableQuery dbQuery,
			AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAggregates) {
		Map<AdhocQueryStep, SliceToValue> queryStepToValues = new HashMap<>();
		dbQuery.getAggregators().forEach(aggregator -> {
			AdhocQueryStep queryStep = AdhocQueryStep.edit(dbQuery).measure(aggregator).build();

			MultiTypeStorage<AdhocSliceAsMap> storage =
					coordinatesToAggregates.getAggregatorToStorage().get(aggregator);

			if (storage == null) {
				// Typically happens when a filter reject completely one of the underlying
				// measure
				storage = MultiTypeStorage.empty();
			}

			eventBus.post(
					QueryStepIsCompleted.builder().querystep(queryStep).nbCells(storage.size()).source(this).build());
			log.debug("dbQuery={} generated a column with size={}", dbQuery, storage.size());

			// The aggregation step is done: the storage is supposed not to be edited: we
			// re-use it in place, to
			// spare a copy to an immutable container
			queryStepToValues.put(queryStep, SliceToValue.builder().storage(storage).build());
		});
		return queryStepToValues;
	}

	protected void walkDagUpToQueriedMeasures(AdhocExecutingQueryContext queryWithContext,
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

			IMeasure measure = queryWithContext.resolveIfRef(queryStep.getMeasure());

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
			IHasUnderlyingQuerySteps hasUnderlyingQuerySteps =
					hasUnderlyingMeasures.wrapNode(operatorsFactory, queryStep);
			ISliceToValue coordinatesToValues;
			try {
				coordinatesToValues = hasUnderlyingQuerySteps.produceOutputColumn(underlyings);
			} catch (RuntimeException e) {
				throw new IllegalStateException(
						"Issue computing columns for m=%s".formatted(queryStep.getMeasure().getName()),
						e);
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

	protected AggregatingMeasurators<AdhocSliceAsMap> sinkToAggregates(TableQuery adhocQuery,
			IRowsStream stream,
			Map<String, Set<Aggregator>> columnToAggregators) {

		AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAgg = new AggregatingMeasurators<>(operatorsFactory);

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
		BiConsumer<Map<String, ?>, Optional<AdhocSliceAsMap>> peekOnCoordinate = prepareStreamLogger(adhocQuery);

		// Process the underlying stream of data to execute aggregations
		try {
			stream.asMap().forEach(input -> {
				forEachStreamedRow(adhocQuery,
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

	protected BiConsumer<Map<String, ?>, Optional<AdhocSliceAsMap>> prepareStreamLogger(TableQuery adhocQuery) {
		AtomicInteger nbIn = new AtomicInteger();
		AtomicInteger nbOut = new AtomicInteger();

		BiConsumer<Map<String, ?>, Optional<AdhocSliceAsMap>> peekOnCoordinate = (input, optCoordinates) -> {

			if (optCoordinates.isEmpty()) {
				// Skip this input as it is incompatible with the groupBy
				// This may not be done by IAdhocDatabaseWrapper for complex groupBys.
				// TODO Wouldn't this be a bug in IAdhocDatabaseWrapper?
				int currentOut = nbOut.incrementAndGet();
				if (adhocQuery.isDebug() && Integer.bitCount(currentOut) == 1) {
					log.info("We rejected {} as row #{}", input, currentOut);
				}
			} else {
				int currentIn = nbIn.incrementAndGet();
				if (adhocQuery.isDebug() && Integer.bitCount(currentIn) == 1) {
					log.info("We accepted {} as row #{}", input, currentIn);
				}
			}
		};
		return peekOnCoordinate;
	}

	protected void forEachStreamedRow(TableQuery tableQuery,
			Map<String, Set<Aggregator>> columnToAggregators,
			Map<String, ?> tableRow,
			BiConsumer<Map<String, ?>, Optional<AdhocSliceAsMap>> peekOnCoordinate,
			Set<String> relevantColumns,
			AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAgg) {
		Optional<AdhocSliceAsMap> optCoordinates = makeCoordinate(tableQuery, tableRow);

		peekOnCoordinate.accept(tableRow, optCoordinates);

		if (optCoordinates.isEmpty()) {
			return;
		}

		// When would we need to filter? As the filter is done by the
		// IAdhocDatabaseWrapper
		// if (!FilterHelpers.match(adhocQuery.getFilter(), input)) {
		// return;
		// }

		AdhocSliceAsMap coordinates = optCoordinates.get();

		for (String aggregatedColumn : relevantColumns) {
			if (tableRow.containsKey(aggregatedColumn)) {
				// We received a row contributing to an aggregate: the DB does not provide
				// aggregates (e.g.
				// InMemoryDb)
				Set<Aggregator> aggs = columnToAggregators.get(aggregatedColumn);

				Object v = tableRow.get(aggregatedColumn);

				if (aggs == null) {
					// DB has done the aggregation for us
					Optional<Aggregator> optAgg = isAggregator(columnToAggregators, aggregatedColumn);

					optAgg.ifPresent(agg -> {
						coordinatesToAgg.contribute(agg, coordinates, v);
					});
				} else {
					// The DB provides the column raw value, and not an aggregated value
					// So we aggregate row values ourselves
					aggs.forEach(agg -> coordinatesToAgg.contribute(agg, coordinates, v));
				}
			}
		}
	}

	protected Optional<Aggregator> isAggregator(Map<String, Set<Aggregator>> columnToAggregators,
			String aggregatorName) {
		return columnToAggregators.values()
				.stream()
				.flatMap(c -> c.stream())
				.filter(a -> a.getName().equals(aggregatorName))
				.findAny();
	}

	/**
	 * @param adhocQuery
	 * @param tableRow
	 * @return the coordinate for given input, or empty if the input is not compatible with given groupBys.
	 */
	protected Optional<AdhocSliceAsMap> makeCoordinate(IWhereGroupbyAdhocQuery adhocQuery, Map<String, ?> tableRow) {
		IAdhocGroupBy groupBy = adhocQuery.getGroupBy();
		if (groupBy.isGrandTotal()) {
			return Optional.of(AdhocSliceAsMap.fromMap(Collections.emptyMap()));
		}

		NavigableSet<String> groupedByColumns = groupBy.getGroupedByColumns();

		AdhocMap.AdhocMapBuilder coordinatesBuilder = AdhocMap.builder(groupedByColumns);

		for (String groupedByColumn : groupedByColumns) {
			Object value = tableRow.get(groupedByColumn);

			if (value == null) {
				if (tableRow.containsKey(groupedByColumn)) {
					// We received an explicit null
					// Typically happens on a failed LEFT JOIN
					value = valueOnNull(groupedByColumn);

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
	protected Object valueOnNull(String column) {
		return missingColumnManager.onMissingColumn(column);
	}

	/**
	 * @param queryWithContext
	 * @return the Set of {@link TableQuery} to be executed.
	 */
	public Set<TableQuery> prepareForTable(AdhocExecutingQueryContext queryWithContext) {
		DagHolder dagHolder = makeQueryStepsDag(queryWithContext);

		if (queryWithContext.isExplain() || queryWithContext.isDebug()) {
			explainDagSteps(dagHolder);
		}

		return queryStepsDagToDbQueries(queryWithContext, dagHolder);

	}

	protected Set<TableQuery> queryStepsDagToDbQueries(AdhocExecutingQueryContext queryWithContext,
			DagHolder dagHolder) {
		// Pack each steps targeting the same groupBy+filters. Multiple measures can be evaluated on such packs.
		Map<MeasurelessQuery, Set<Aggregator>> measurelessToAggregators = new HashMap<>();

		// https://stackoverflow.com/questions/57134161/how-to-find-roots-and-leaves-set-in-jgrapht-directedacyclicgraph
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> dag = dagHolder.getDag();
		dag.vertexSet().stream().filter(step -> dag.outgoingEdgesOf(step).isEmpty()).forEach(step -> {
			IMeasure leafMeasure = queryWithContext.resolveIfRef(step.getMeasure());

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

	protected DagHolder makeQueryStepsDag(AdhocExecutingQueryContext queryWithContext) {
		QueryStepsDagsBuilder queryStepsDagBuilder = makeQueryStepsDagsBuilder(queryWithContext.getQuery());

		// Add explicitly requested steps
		Set<IMeasure> queriedMeasures = convertToQueriedSteps(queryWithContext);
		queriedMeasures.forEach(queryStepsDagBuilder::addRoot);

		// Add implicitly requested steps
		while (queryStepsDagBuilder.hasLeftovers()) {
			AdhocQueryStep adhocSubQuery = queryStepsDagBuilder.pollLeftover();

			IMeasure measure = queryWithContext.resolveIfRef(adhocSubQuery.getMeasure());

			if (measure instanceof Aggregator aggregator) {
				log.debug("Aggregators (here {}) do not have any underlying measure", aggregator);
			} else if (measure instanceof IHasUnderlyingMeasures measureWithUnderlyings) {
				IHasUnderlyingQuerySteps wrappedQueryStep =
						measureWithUnderlyings.wrapNode(operatorsFactory, adhocSubQuery);
				for (AdhocQueryStep underlyingStep : wrappedQueryStep.getUnderlyingSteps()) {
					// Make sure the DAG has actual measure nodes, and not references
					IMeasure notRefMeasure = queryWithContext.resolveIfRef(underlyingStep.getMeasure());
					underlyingStep = AdhocQueryStep.edit(underlyingStep).measure(notRefMeasure).build();

					queryStepsDagBuilder.addEdge(adhocSubQuery, underlyingStep);
				}
			} else {
				throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(measure).toString());
			}
		}

		queryStepsDagBuilder.sanityChecks();

		return queryStepsDagBuilder.getQueryDag();
	}

	private Set<IMeasure> convertToQueriedSteps(AdhocExecutingQueryContext queryWithContext) {
		Set<ReferencedMeasure> measureRefs = queryWithContext.getQuery().getMeasureRefs();
		Set<IMeasure> queriedMeasures;
		if (measureRefs.isEmpty()) {
			IMeasure countAsterisk = defaultMeasure();
			queriedMeasures = Set.of(countAsterisk);
		} else {
			queriedMeasures =
					measureRefs.stream().map(ref -> queryWithContext.resolveIfRef(ref)).collect(Collectors.toSet());
		}
		return queriedMeasures;
	}

	// Not a single measure is selected: we are doing a DISTINCT query

	/**
	 *
	 * @return the measure to be considered if not measure is provided to the query
	 */
	protected IMeasure defaultMeasure() {
		return Aggregator.builder().name("COUNT_ASTERISK").aggregationKey("COUNT").columnName("*").build();
		// return
		// Aggregator.builder().name("CONSTANT_1").aggregationKey("COUNT").columnName("*").build();
	}

	protected QueryStepsDagsBuilder makeQueryStepsDagsBuilder(IAdhocQuery adhocQuery) {
		return new QueryStepsDagsBuilder(adhocQuery);
	}

	public static AdhocQueryEngineBuilder edit(AdhocQueryEngine engine) {
		return AdhocQueryEngine.builder()
				.operatorsFactory(engine.operatorsFactory)
				.eventBus(engine.eventBus)
				.missingColumnManager(engine.missingColumnManager);
	}
}
