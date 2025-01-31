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

import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.StandardOperatorsFactory;
import eu.solven.adhoc.aggregations.collection.UnionSetAggregator;
import eu.solven.adhoc.database.IAdhocTableWrapper;
import eu.solven.adhoc.database.IRowsStream;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.cube.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.storage.AggregatingMeasurators;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import eu.solven.adhoc.storage.ValueConsumer;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Columnator;
import eu.solven.adhoc.transformers.EmptyMeasure;
import eu.solven.adhoc.transformers.IHasUnderlyingMeasures;
import eu.solven.adhoc.transformers.IHasUnderlyingQuerySteps;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import eu.solven.adhoc.view.ITabularView;
import eu.solven.adhoc.view.MapBasedTabularView;
import eu.solven.adhoc.view.RowScanner;
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
	final IAdhocEventBus eventBus;

	@Override
	public ITabularView execute(AdhocExecutingQueryContext queryWithContext, IAdhocTableWrapper table) {
		Set<TableQuery> prepared = prepare(queryWithContext);

		Map<TableQuery, IRowsStream> dbQueryToStream = new HashMap<>();
		for (TableQuery dbQuery : prepared) {
			dbQueryToStream.put(dbQuery, table.openDbStream(dbQuery));
		}

		return execute(queryWithContext, dbQueryToStream);
	}

	protected ITabularView execute(AdhocExecutingQueryContext queryWithContext,
			Map<TableQuery, IRowsStream> dbQueryToSteam) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates = makeQueryStepsDag(queryWithContext);

		Map<String, Set<Aggregator>> inputColumnToAggregators =
				columnToAggregators(queryWithContext, fromQueriedToAggregates);

		Map<AdhocQueryStep, ICoordinatesToValues> queryStepToValues = new LinkedHashMap<>();

		// This is the only step consuming the input stream
		dbQueryToSteam.forEach((dbQuery, stream) -> {
			Map<AdhocQueryStep, CoordinatesToValues> oneQueryStepToValues =
					aggregateStreamToAggregates(dbQuery, stream, inputColumnToAggregators);

			queryStepToValues.putAll(oneQueryStepToValues);
		});

		if (queryWithContext.isDebug()) {
			queryStepToValues.forEach((aggregateStep, values) -> {
				values.scan(row -> {
					return AsObjectValueConsumer.consumer(o -> {
						log.info("[DEBUG] {} -> {} step={}", o, row, aggregateStep);
					});
				});

			});
		}

		// We're done with the input stream: the DB can be shutdown, we could answer the
		// query
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregates").source(this).build());

		walkDagUpToQueriedMeasures(queryWithContext, fromQueriedToAggregates, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transformations").source(this).build());

		MapBasedTabularView mapBasedTabularView =
				toTabularView(queryWithContext.getQueryOptions(), fromQueriedToAggregates, queryStepToValues);

		return mapBasedTabularView;
	}

	protected Map<String, Set<Aggregator>> columnToAggregators(AdhocExecutingQueryContext queryWithContext,
			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates) {
		Map<String, Set<Aggregator>> columnToAggregators = new LinkedHashMap<>();

		fromQueriedToAggregates.vertexSet()
				.stream()
				.filter(step -> fromQueriedToAggregates.outDegreeOf(step) == 0)
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

	protected MapBasedTabularView toTabularView(Set<? extends IQueryOption> queryOptions,
			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates,
			Map<AdhocQueryStep, ICoordinatesToValues> queryStepToValues) {
		MapBasedTabularView mapBasedTabularView = MapBasedTabularView.builder().build();

		Iterator<AdhocQueryStep> stepsToReturn;
		if (queryOptions.contains(StandardQueryOptions.RETURN_UNDERLYING_MEASURES)) {
			// BEWARE Should we return steps with same groupBy?
			// What about measures appearing multiple times in the DAG?
			stepsToReturn = new BreadthFirstIterator<>(fromQueriedToAggregates);
		} else {
			stepsToReturn = fromQueriedToAggregates.vertexSet()
					.stream()
					.filter(step -> fromQueriedToAggregates.inDegreeOf(step) == 0)
					.iterator();
		}

		stepsToReturn.forEachRemaining(step -> {
			RowScanner<AdhocSliceAsMap> rowScanner = new RowScanner<AdhocSliceAsMap>() {

				@Override
				public ValueConsumer onKey(AdhocSliceAsMap coordinates) {
					AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
						mapBasedTabularView.append(coordinates, Map.of(step.getMeasure().getName(), o));
					});

					return consumer;
				}
			};

			ICoordinatesToValues coordinatesToValues = queryStepToValues.get(step);
			if (coordinatesToValues == null) {
				// Happens on a Columnator missing a required column
			} else {
				coordinatesToValues.scan(rowScanner);
			}
		});
		return mapBasedTabularView;
	}

	protected Map<AdhocQueryStep, CoordinatesToValues> aggregateStreamToAggregates(TableQuery dbQuery,
			IRowsStream stream,
			Map<String, Set<Aggregator>> columnToAggregators) {

		AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAggregates =
				sinkToAggregates(dbQuery, stream, columnToAggregators);

		return toImmutableChunks(dbQuery, coordinatesToAggregates);
	}

	protected Map<AdhocQueryStep, CoordinatesToValues> toImmutableChunks(TableQuery dbQuery,
			AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAggregates) {
		Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues = new HashMap<>();
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
			queryStepToValues.put(queryStep, CoordinatesToValues.builder().storage(storage).build());
		});
		return queryStepToValues;
	}

	protected void walkDagUpToQueriedMeasures(AdhocExecutingQueryContext queryWithContext,
			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates,
			Map<AdhocQueryStep, ICoordinatesToValues> queryStepToValues) {
		// https://stackoverflow.com/questions/69183360/traversal-of-edgereversedgraph
		EdgeReversedGraph<AdhocQueryStep, DefaultEdge> fromAggregatesToQueried =
				new EdgeReversedGraph<>(fromQueriedToAggregates);

		// https://en.wikipedia.org/wiki/Topological_sorting
		// TopologicalOrder guarantees processing a vertex after dependent vertices are
		// done.
		TopologicalOrderIterator<AdhocQueryStep, DefaultEdge> graphIterator =
				new TopologicalOrderIterator<>(fromAggregatesToQueried);

		graphIterator.forEachRemaining(queryStep -> {

			if (queryStepToValues.containsKey(queryStep)) {
				// This typically happens on aggregator measures, as they are fed in a previous
				// step
				// Here, we want to process a measure once its underlying steps are completed
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

			// Map<String, AdhocQueryStep> underlyingToStep = new HashMap<>();
			// underlyingSteps.forEach(step -> {
			// String underlyingName = resolveIfRef(step.getMeasure()).getName();
			// AdhocQueryStep removed = underlyingToStep.put(underlyingName, step);
			// if (removed != null) {
			// // TODO Is this a legit case for Dispatcher?
			// throw new IllegalArgumentException("Multiple steps for %s".formatted(underlyingName));
			// }
			// });

			if (underlyingSteps.isEmpty()) {
				// This may happen on a Columnator which is missing a required column
				return;
			} else if (measure instanceof IHasUnderlyingMeasures hasUnderlyingMeasures) {
				List<ICoordinatesToValues> underlyings = underlyingSteps.stream().map(underlyingStep -> {
					ICoordinatesToValues values = queryStepToValues.get(underlyingStep);

					if (values == null) {
						throw new IllegalStateException("The DAG missed values for step=%s".formatted(underlyingStep));
					}

					return values;
				}).toList();

				// BEWARE It looks weird we have to call again `.wrapNode`
				IHasUnderlyingQuerySteps hasUnderlyingQuerySteps =
						hasUnderlyingMeasures.wrapNode(operatorsFactory, queryStep);
				ICoordinatesToValues coordinatesToValues = hasUnderlyingQuerySteps.produceOutputColumn(underlyings);

				eventBus.post(QueryStepIsCompleted.builder()
						.querystep(queryStep)
						.nbCells(coordinatesToValues.keySet().size())
						.source(this)
						.build());

				queryStepToValues.put(queryStep, coordinatesToValues);
			} else {
				throw new UnsupportedOperationException("%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
			}
		});
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
		stream.asMap().forEach(input -> {
			forEachStreamedRow(adhocQuery,
					columnToAggregators,
					input,
					peekOnCoordinate,
					relevantColumns,
					coordinatesToAgg);
		});

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

	protected void forEachStreamedRow(TableQuery adhocQuery,
			Map<String, Set<Aggregator>> columnToAggregators,
			Map<String, ?> input,
			BiConsumer<Map<String, ?>, Optional<AdhocSliceAsMap>> peekOnCoordinate,
			Set<String> relevantColumns,
			AggregatingMeasurators<AdhocSliceAsMap> coordinatesToAgg) {
		Optional<AdhocSliceAsMap> optCoordinates = makeCoordinate(adhocQuery, input);

		peekOnCoordinate.accept(input, optCoordinates);

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
			if (input.containsKey(aggregatedColumn)) {
				// We received a row contributing to an aggregate: the DB does not provide
				// aggregates (e.g.
				// InMemoryDb)
				Set<Aggregator> aggs = columnToAggregators.get(aggregatedColumn);

				Object v = input.get(aggregatedColumn);

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
	 * @param input
	 * @return the coordinate for given input, or empty if the input is not compatible with given groupBys.
	 */
	protected Optional<AdhocSliceAsMap> makeCoordinate(IWhereGroupbyAdhocQuery adhocQuery, Map<String, ?> input) {
		if (adhocQuery.getGroupBy().isGrandTotal()) {
			return Optional.of(AdhocSliceAsMap.fromMap(Collections.emptyMap()));
		}

		NavigableSet<String> groupedByColumns = adhocQuery.getGroupBy().getGroupedByColumns();

		Map<String, Object> coordinates = new LinkedHashMap<>(groupedByColumns.size());

		for (String groupBy : groupedByColumns) {
			Object value = input.get(groupBy);

			if (value == null) {
				if (input.containsKey(groupBy)) {
					// We received an explicit null
					// Typically happens on a failed LEFT JOIN
					value = valueOnNull(groupBy);

					assert value != null : "`null` is not a legal column value";
				} else {
					// The input lack a groupBy coordinate: we exclude it
					// TODO What's a legitimate case leading to this?
					return Optional.empty();
				}
			}

			coordinates.put(groupBy, value);
		}

		return Optional.of(AdhocSliceAsMap.fromMap(coordinates));
	}

	/**
	 * The value to inject in place of a NULL. Returning a null-reference is not supported.
	 *
	 * @param column
	 *            the column over which a null is encountered. You may customize `null` behavior on a per-column basis.
	 */
	protected Object valueOnNull(String column) {
		return "NULL";
	}

	/**
	 * @param queryWithContext
	 * @return the Set of {@link TableQuery} to be executed.
	 */
	public Set<TableQuery> prepare(AdhocExecutingQueryContext queryWithContext) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> directedGraph = makeQueryStepsDag(queryWithContext);

		return queryStepsDagToDbQueries(queryWithContext, directedGraph);

	}

	protected Set<TableQuery> queryStepsDagToDbQueries(AdhocExecutingQueryContext queryWithContext,
			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> directedGraph) {
		Map<MeasurelessQuery, Set<Aggregator>> measurelessToAggregators = new HashMap<>();

		// https://stackoverflow.com/questions/57134161/how-to-find-roots-and-leaves-set-in-jgrapht-directedacyclicgraph
		directedGraph.vertexSet()
				.stream()
				.filter(step -> directedGraph.outgoingEdgesOf(step).isEmpty())
				.forEach(step -> {
					IMeasure leafMeasure = queryWithContext.resolveIfRef(step.getMeasure());

					if (leafMeasure instanceof Aggregator leafAggregator) {
						MeasurelessQuery measureless = MeasurelessQuery.edit(step).build();

						// We could analyze filters, to discard a query filter `k=v` if another query
						// filters `k=v|v2`
						measurelessToAggregators.merge(measureless,
								Collections.singleton(leafAggregator),
								UnionSetAggregator::unionSet);
					} else if (leafMeasure instanceof EmptyMeasure) {
						// ???
					} else if (leafMeasure instanceof Columnator) {
						// ???
						// Happens if we miss given column
					} else {
						throw new IllegalStateException("Expected simple aggregators. Got %s".formatted(leafMeasure));
					}
				});

		boolean explain = queryWithContext.isExplain();
		boolean debug = queryWithContext.isDebug();

		if (explain || debug) {
			new TopologicalOrderIterator<>(directedGraph).forEachRemaining(step -> {
				Set<DefaultEdge> underlyings = directedGraph.outgoingEdgesOf(step);

				underlyings.forEach(edge -> log
						.info("[EXPLAIN] {} -> {}", step, Graphs.getOppositeVertex(directedGraph, edge, step)));
			});
		}

		return measurelessToAggregators.entrySet().stream().map(e -> {
			MeasurelessQuery adhocLeafQuery = e.getKey();
			Set<Aggregator> leafAggregators = e.getValue();
			return TableQuery.edit(adhocLeafQuery)
					.aggregators(leafAggregators)
					.explain(explain)
					.debug(debug)
					.customMarker(adhocLeafQuery.getCustomMarker())
					.build();
		}).collect(Collectors.toSet());
	}

	protected DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> makeQueryStepsDag(
			AdhocExecutingQueryContext queryWithContext) {
		QueryStepsDagsBuilder queryStepsDagBuilder = makeQueryStepsDagsBuilder(queryWithContext.getAdhocQuery());

		// Add explicitly requested steps
		Set<ReferencedMeasure> measureRefs = queryWithContext.getAdhocQuery().getMeasureRefs();
		if (measureRefs.isEmpty()) {
			IMeasure countAsterisk = defaultMeasure();
			queryStepsDagBuilder.addRoot(countAsterisk);
		} else {
			measureRefs.stream().map(ref -> queryWithContext.resolveIfRef(ref)).forEach(queryStepsDagBuilder::addRoot);
		}

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

	// Not a single measure is selected: we are doing a DISTINCT query

	/**
	 *
	 * @return te measure to be considered if not measure is provided to the query
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
		return AdhocQueryEngine.builder().operatorsFactory(engine.operatorsFactory).eventBus(engine.eventBus);
	}
}
