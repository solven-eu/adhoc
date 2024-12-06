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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.greenrobot.eventbus.EventBus;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;

import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.RowScanner;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.StandardOperatorsFactory;
import eu.solven.adhoc.aggregations.collection.UnionSetAggregator;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.database.IAdhocDatabaseWrapper;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.MeasuratorIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.storage.AggregatingMeasurators;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import eu.solven.adhoc.storage.ValueConsumer;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Columnator;
import eu.solven.adhoc.transformers.EmptyMeasure;
import eu.solven.adhoc.transformers.IHasUnderlyingMeasures;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
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
	final AdhocMeasureBag measureBag;

	@NonNull
	final EventBus eventBus;

	public ITabularView execute(IAdhocQuery adhocQuery, IAdhocDatabaseWrapper db) {
		return execute(adhocQuery, Set.of(), db);
	}

	public ITabularView execute(IAdhocQuery adhocQuery,
			Set<? extends IQueryOption> queryOptions,
			IAdhocDatabaseWrapper db) {
		Set<DatabaseQuery> prepared = prepare(queryOptions, adhocQuery);

		Map<DatabaseQuery, Stream<Map<String, ?>>> dbQueryToStream = new HashMap<>();
		for (DatabaseQuery dbQuery : prepared) {
			dbQueryToStream.put(dbQuery, db.openDbStream(dbQuery));
		}

		return execute(adhocQuery, queryOptions, dbQueryToStream);
	}

	protected ITabularView execute(IAdhocQuery adhocQuery,
			Set<? extends IQueryOption> queryOptions,
			Map<DatabaseQuery, Stream<Map<String, ?>>> dbQueryToSteam) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates =
				makeQueryStepsDag(queryOptions, adhocQuery);

		Map<String, Set<Aggregator>> inputColumnToAggregators = columnToAggregators(fromQueriedToAggregates);

		Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues = new LinkedHashMap<>();

		// This is the only step consuming the input stream
		dbQueryToSteam.forEach((dbQuery, stream) -> {
			Map<AdhocQueryStep, CoordinatesToValues> oneQueryStepToValues =
					aggregateStreamToAggregates(dbQuery, stream, inputColumnToAggregators);

			queryStepToValues.putAll(oneQueryStepToValues);
		});

		if (adhocQuery.isDebug()) {
			queryStepToValues.forEach((aggregateStep, values) -> {
				values.scan(row -> {
					return AsObjectValueConsumer.consumer(o -> {
						log.info("[DEBUG] {} -> {} step={}", o, row, aggregateStep);
					});
				});

			});
		}

		// We're done with the input stream: the DB can be shutdown, we could answer the query
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregates").source(this).build());

		walkDagUpToQueriedMeasures(fromQueriedToAggregates, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transformations").source(this).build());

		MapBasedTabularView mapBasedTabularView =
				toTabularView(queryOptions, fromQueriedToAggregates, queryStepToValues);

		return mapBasedTabularView;
	}

	protected Map<String, Set<Aggregator>> columnToAggregators(
			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates) {
		Map<String, Set<Aggregator>> columnToAggregators = new LinkedHashMap<>();

		fromQueriedToAggregates.vertexSet()
				.stream()
				.filter(step -> fromQueriedToAggregates.outDegreeOf(step) == 0)
				.map(AdhocQueryStep::getMeasure)
				.forEach(measure -> {
					measure = resolveIfRef(measure);

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
			Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues) {
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
			RowScanner<Map<String, ?>> rowScanner = new RowScanner<Map<String, ?>>() {

				@Override
				public ValueConsumer onKey(Map<String, ?> coordinates) {
					AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
						mapBasedTabularView.append(coordinates, Map.of(step.getMeasure().getName(), o));
					});

					return consumer;
				}
			};

			CoordinatesToValues coordinatesToValues = queryStepToValues.get(step);
			if (coordinatesToValues == null) {
				// Happens on a Columnator missing a required column
			} else {
				coordinatesToValues.scan(rowScanner);
			}
		});
		return mapBasedTabularView;
	}

	protected Map<AdhocQueryStep, CoordinatesToValues> aggregateStreamToAggregates(DatabaseQuery dbQuery,
			Stream<Map<String, ?>> stream,
			Map<String, Set<Aggregator>> columnToAggregators) {

		AggregatingMeasurators<Map<String, ?>> coordinatesToAggregates =
				sinkToAggregates(dbQuery, stream, columnToAggregators);

		return toImmutableChunks(dbQuery, coordinatesToAggregates);
	}

	protected Map<AdhocQueryStep, CoordinatesToValues> toImmutableChunks(DatabaseQuery dbQuery,
			AggregatingMeasurators<Map<String, ?>> coordinatesToAggregates) {
		Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues = new HashMap<>();
		dbQuery.getAggregators().forEach(aggregator -> {
			AdhocQueryStep adhocStep = AdhocQueryStep.edit(dbQuery).measure(aggregator).build();

			MultiTypeStorage<Map<String, ?>> storage = coordinatesToAggregates.getAggregatorToStorage().get(aggregator);

			if (storage == null) {
				// Typically happens when a filter reject completely one of the underlying measure
				storage = MultiTypeStorage.empty();
			}

			eventBus.post(
					MeasuratorIsCompleted.builder().measure(aggregator).nbCells(storage.size()).source(this).build());
			log.debug("dbQuery={} generated a column with size={}", dbQuery, storage.size());

			// The aggregation step is done: the storage is supposed not to be edited: we re-use it in place, to
			// spare a copy to an immutable container
			queryStepToValues.put(adhocStep, CoordinatesToValues.builder().storage(storage).build());
		});
		return queryStepToValues;
	}

	protected void walkDagUpToQueriedMeasures(DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates,
			Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues) {
		// https://stackoverflow.com/questions/69183360/traversal-of-edgereversedgraph
		EdgeReversedGraph<AdhocQueryStep, DefaultEdge> fromAggregatesToQueried =
				new EdgeReversedGraph<>(fromQueriedToAggregates);

		// https://en.wikipedia.org/wiki/Topological_sorting
		// TopologicalOrder guarantees processing a vertex after dependent vertices are done.
		TopologicalOrderIterator<AdhocQueryStep, DefaultEdge> graphIterator =
				new TopologicalOrderIterator<>(fromAggregatesToQueried);

		graphIterator.forEachRemaining(queryStep -> {

			if (queryStepToValues.containsKey(queryStep)) {
				// This typically happens on aggregator measures, as they are fed in a previous step
				return;
			} else if (queryStep.getMeasure() instanceof Aggregator a) {
				throw new IllegalStateException("Missing values for %s".formatted(a));
			}

			eventBus.post(QueryStepIsEvaluating.builder().queryStep(queryStep).source(this).build());

			IMeasure measure = resolveIfRef(queryStep.getMeasure());

			Set<DefaultEdge> underlyingStepEdges = fromAggregatesToQueried.incomingEdgesOf(queryStep);
			List<AdhocQueryStep> underlyingSteps = underlyingStepEdges.stream()
					.map(edge -> Graphs.getOppositeVertex(fromAggregatesToQueried, edge, queryStep))
					.toList();

			Map<String, AdhocQueryStep> underlyingToStep = new HashMap<>();
			underlyingSteps.forEach(step -> {
				String underlyingName = resolveIfRef(step.getMeasure()).getName();
				AdhocQueryStep removed = underlyingToStep.put(underlyingName, step);
				if (removed != null) {
					// TODO Is this a legit case for Dispatcher?
					throw new IllegalArgumentException("Multiple steps for %s".formatted(underlyingName));
				}
			});

			if (underlyingSteps.isEmpty()) {
				// This may happen on a Columnator which is missing a required column
				return;
			} else if (measure instanceof IHasUnderlyingMeasures hasUnderlyingMeasures) {
				List<CoordinatesToValues> underlyings =
						hasUnderlyingMeasures.getUnderlyingNames().stream().map(underlyingToStep::get).map(step -> {
							CoordinatesToValues values = queryStepToValues.get(step);

							if (values == null) {
								throw new IllegalStateException("The DAG missed step=%s".formatted(step));
							}

							return values;
						}).collect(Collectors.toList());

				CoordinatesToValues coordinatesToValues =
						hasUnderlyingMeasures.wrapNode(operatorsFactory, queryStep).produceOutputColumn(underlyings);

				eventBus.post(MeasuratorIsCompleted.builder()
						.measure(measure)
						.nbCells(coordinatesToValues.getStorage().size())
						.source(this)
						.build());

				queryStepToValues.put(queryStep, coordinatesToValues);
			} else {
				throw new UnsupportedOperationException("%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
			}
		});
	}

	protected IMeasure resolveIfRef(IMeasure measure) {
		return resolveIfRef(Set.of(), measure);
	}

	protected IMeasure resolveIfRef(Set<? extends IQueryOption> queryOptions, IMeasure measure) {
		if (measure == null) {
			throw new IllegalArgumentException("Null input");
		}

		if (queryOptions.contains(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY)) {
			if (measure instanceof ReferencedMeasure ref) {
				return this.measureBag.resolveIfRefOpt(ref).orElseGet(() -> new EmptyMeasure(ref.getRef()));
			} else {
				return this.measureBag.resolveIfRefOpt(measure).orElseGet(() -> new EmptyMeasure(measure.getName()));
			}
		} else {
			return this.measureBag.resolveIfRef(measure);
		}
	}

	protected AggregatingMeasurators<Map<String, ?>> sinkToAggregates(DatabaseQuery adhocQuery,
			Stream<Map<String, ?>> stream,
			Map<String, Set<Aggregator>> columnToAggregators) {

		AggregatingMeasurators<Map<String, ?>> coordinatesToAgg = new AggregatingMeasurators<>(operatorsFactory);

		AtomicInteger nbIn = new AtomicInteger();
		AtomicInteger nbOut = new AtomicInteger();

		// Process the underlying stream of data to execute aggregations
		stream.forEach(input -> {
			Optional<Map<String, ?>> optCoordinates = makeCoordinate(adhocQuery, input);

			if (optCoordinates.isEmpty()) {
				// Skip this input as it is incompatible with the groupBy
				// This may not be done by IAdhocDatabaseWrapper for complex groupBys.
				// TODO Wouldn't this be a bug in IAdhocDatabaseWrapper?
				int currentOut = nbOut.incrementAndGet();
				if (adhocQuery.isDebug() && Integer.bitCount(currentOut) == 1) {
					log.info("We rejected {} as row #{}", input, currentOut);
				}
				return;
			} else {
				int currentIn = nbIn.incrementAndGet();
				if (adhocQuery.isDebug() && Integer.bitCount(currentIn) == 1) {
					log.info("We accepted {} as row #{}", input, currentIn);
				}
			}

			// When would we need to filter? As the filter is done by the IAdhocDatabaseWrapper
			// if (!FilterHelpers.match(adhocQuery.getFilter(), input)) {
			// return;
			// }

			// Iterate either on input map or on requested measures, depending on map sizes
			if (columnToAggregators.size() < input.size()) {
				columnToAggregators.forEach((aggName, aggs) -> {
					if (input.containsKey(aggName)) {
						Object v = input.get(aggName);

						aggs.forEach(agg -> coordinatesToAgg.contribute(agg, optCoordinates.get(), v));
					}
				});
			} else {
				input.forEach((k, v) -> {
					if (columnToAggregators.containsKey(k)) {
						Set<Aggregator> aggs = columnToAggregators.get(k);

						aggs.forEach(agg -> coordinatesToAgg.contribute(agg, optCoordinates.get(), v));
					}
				});
			}
		});

		return coordinatesToAgg;
	}

	/**
	 * @param adhocQuery
	 * @param input
	 * @return the coordinate for given input, or empty if the input is not compatible with given groupBys.
	 */
	protected Optional<Map<String, ?>> makeCoordinate(IWhereGroupbyAdhocQuery adhocQuery, Map<String, ?> input) {
		if (adhocQuery.getGroupBy().isGrandTotal()) {
			return Optional.of(Collections.emptyMap());
		}

		NavigableSet<String> groupedByColumns = adhocQuery.getGroupBy().getGroupedByColumns();

		Map<String, Object> coordinates = new LinkedHashMap<>(groupedByColumns.size());

		for (String groupBy : groupedByColumns) {
			Object value = input.get(groupBy);

			if (value == null) {
				// The input lack a groupBy coordinate: we exclude it
				return Optional.empty();
			}

			coordinates.put(groupBy, value);
		}

		return Optional.of(coordinates);
	}

	/**
	 * @param queryOptions
	 * @param adhocQuery
	 * @return the Set of {@link IAdhocQuery} to be executed to an underlying Database to be able to execute the
	 *         {@link DAGForQuery}
	 */
	public Set<DatabaseQuery> prepare(Set<? extends IQueryOption> queryOptions, IAdhocQuery adhocQuery) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> directedGraph = makeQueryStepsDag(queryOptions, adhocQuery);

		return queryStepsDagToDbQueries(directedGraph, adhocQuery.isExplain(), adhocQuery.isDebug());

	}

	protected Set<DatabaseQuery> queryStepsDagToDbQueries(
			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> directedGraph,
			boolean explain,
			boolean debug) {
		Map<MeasurelessQuery, Set<Aggregator>> measurelessToAggregators = new HashMap<>();

		// https://stackoverflow.com/questions/57134161/how-to-find-roots-and-leaves-set-in-jgrapht-directedacyclicgraph
		directedGraph.vertexSet()
				.stream()
				.filter(step -> directedGraph.outgoingEdgesOf(step).size() == 0)
				.forEach(step -> {
					IMeasure leafMeasure = resolveIfRef(step.getMeasure());

					if (leafMeasure instanceof Aggregator leafAggregator) {
						MeasurelessQuery measureless = MeasurelessQuery.of(step);

						// We could analyze filters, to discard a query filter `k=v` if another query filters `k=v|v2`
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
			return DatabaseQuery.edit(adhocLeafQuery)
					.aggregators(leafAggregators)
					.explain(explain)
					.debug(debug)
					.build();
		}).collect(Collectors.toSet());
	}

	protected DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> makeQueryStepsDag(
			Set<? extends IQueryOption> queryOptions,
			IAdhocQuery adhocQuery) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> queryDag = new DirectedAcyclicGraph<>(DefaultEdge.class);

		LinkedList<AdhocQueryStep> collectors = new LinkedList<>();

		{

			adhocQuery.getMeasureRefs().stream().map(ref -> resolveIfRef(queryOptions, ref)).forEach(queriedMeasure -> {
				AdhocQueryStep rootStep = AdhocQueryStep.builder()
						.filter(adhocQuery.getFilter())
						.groupBy(adhocQuery.getGroupBy())
						.measure(queriedMeasure)
						.debug(adhocQuery.isDebug())
						.build();

				queryDag.addVertex(rootStep);
				collectors.add(rootStep);
			});

		}

		while (!collectors.isEmpty()) {
			AdhocQueryStep adhocSubQuery = collectors.poll();

			IMeasure measure = resolveIfRef(adhocSubQuery.getMeasure());

			if (measure instanceof Aggregator aggregator) {
				log.debug("Aggregators (here {}) do not have any underlying measure", aggregator);
			} else if (measure instanceof IHasUnderlyingMeasures combinator) {
				for (AdhocQueryStep underlyingStep : combinator.wrapNode(operatorsFactory, adhocSubQuery)
						.getUnderlyingSteps()) {
					// Make sure the DAG has actual measure nodes, and not references
					IMeasure notRefMeasure = resolveIfRef(underlyingStep.getMeasure());
					underlyingStep = AdhocQueryStep.edit(underlyingStep).measure(notRefMeasure).build();

					queryDag.addVertex(underlyingStep);
					queryDag.addEdge(adhocSubQuery, underlyingStep);

					collectors.add(underlyingStep);
				}
			} else {
				throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(measure).toString());
			}
		}

		queryDag.vertexSet().forEach(step -> {
			if (step.getMeasure() instanceof ReferencedMeasure) {
				throw new IllegalStateException("The DAG must not rely on ReferencedMeasure");
			}
		});

		return queryDag;
	}

}
