package eu.solven.adhoc.dag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
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
import eu.solven.adhoc.aggregations.ITransformationFactory;
import eu.solven.adhoc.aggregations.StandardTransformationFactory;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.database.IAdhocDatabase;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.MeasuratorIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.execute.FilterHelpers;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.storage.AggregatingMeasurators;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import eu.solven.adhoc.storage.ValueConsumer;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.IHasUnderlyingMeasures;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import eu.solven.pepper.logging.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The set of named {@link IMeasure}. Many of them are {@link IHasUnderlyingMeasures}, which express a
 * Directed-Acyclic-Graph. The DAG is actually evaluated on a per-query basis.
 * 
 * @author Benoit Lacelle
 *
 */
@RequiredArgsConstructor
@Slf4j
@Builder
public class DAG {
	final EventBus eventBus;

	@Default
	final ITransformationFactory transformationFactory = new StandardTransformationFactory();

	final Map<String, IMeasure> nameToMeasure = new HashMap<>();

	public DAG addMeasure(IMeasure namedMeasure) {
		nameToMeasure.put(namedMeasure.getName(), namedMeasure);

		return this;
	}

	public static <T> List<T> unionList(List<T> left, List<T> right) {
		if (left.isEmpty()) {
			return right;
		} else if (right.isEmpty()) {
			return left;
		}

		List<T> union = new ArrayList<>();

		union.addAll(left);
		union.addAll(right);

		return union;
	}

	public static <T> Set<T> unionSet(Set<T> left, Set<T> right) {
		if (left.isEmpty()) {
			return right;
		} else if (right.isEmpty()) {
			return left;
		}

		Set<T> union = new LinkedHashSet<>();

		union.addAll(left);
		union.addAll(right);

		return union;
	}

	private ITabularView execute(IAdhocQuery adhocQuery,
			Set<? extends IQueryOption> queryOptions,
			Map<DatabaseQuery, Stream<Map<String, ?>>> dbQueryToSteam) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates = makeQueryStepsDag(adhocQuery);

		Map<String, Set<Aggregator>> inputColumnToAggregators = columnToAggregators(fromQueriedToAggregates);

		Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues = new LinkedHashMap<>();

		// This is the only step consuming the input stream
		dbQueryToSteam.forEach((dbQuery, stream) -> {
			Map<AdhocQueryStep, CoordinatesToValues> oneQueryStepToValues =
					aggregateStreamToAggregates(dbQuery, stream, inputColumnToAggregators);

			queryStepToValues.putAll(oneQueryStepToValues);
		});

		// We're done with the input stream: the DB can be shutdown, we could answer the query
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregates").build());

		walkDagUpToQueriedMeasures(fromQueriedToAggregates, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transformations").build());

		MapBasedTabularView mapBasedTabularView =
				toTabularView(queryOptions, fromQueriedToAggregates, queryStepToValues);

		return mapBasedTabularView;
	}

	private Map<String, Set<Aggregator>> columnToAggregators(
			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates) {
		Map<String, Set<Aggregator>> inputColumnToAggregators = new LinkedHashMap<>();

		fromQueriedToAggregates.vertexSet()
				.stream()
				.filter(step -> fromQueriedToAggregates.outDegreeOf(step) == 0)
				.map(step -> step.getMeasure())
				.forEach(measure -> {
					measure = resolveIfRef(measure);

					if (measure instanceof Aggregator aggregator) {
						inputColumnToAggregators.merge(aggregator.getName(), Set.of(aggregator), DAG::unionSet);
					} else {
						throw new UnsupportedOperationException(
								"%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
					}
				});
		return inputColumnToAggregators;
	}

	private MapBasedTabularView toTabularView(Set<? extends IQueryOption> queryOptions,
			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates,
			Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues) {
		MapBasedTabularView mapBasedTabularView = MapBasedTabularView.builder().build();

		Iterator<AdhocQueryStep> measuresToReturn;
		if (queryOptions.contains(StandardQueryOptions.RETURN_UNDERLYING_MEASURES)) {
			// BEWARE Should we return steps with same groupBy?
			// What about measures appearing multiple times in the DAG?
			measuresToReturn = new BreadthFirstIterator<>(fromQueriedToAggregates);
		} else {
			measuresToReturn = fromQueriedToAggregates.vertexSet()
					.stream()
					.filter(step -> fromQueriedToAggregates.inDegreeOf(step) == 0)
					.iterator();
		}

		measuresToReturn.forEachRemaining(step -> {
			RowScanner<Map<String, ?>> rowScanner = new RowScanner<Map<String, ?>>() {

				@Override
				public ValueConsumer onKey(Map<String, ?> coordinates) {
					AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
						mapBasedTabularView.append(coordinates, Map.of(step.getMeasure().getName(), o));
					});

					return consumer;
				}
			};

			queryStepToValues.get(step).scan(rowScanner);
		});
		return mapBasedTabularView;
	}

	private Map<AdhocQueryStep, CoordinatesToValues> aggregateStreamToAggregates(DatabaseQuery dbQuery,
			Stream<Map<String, ?>> stream,
			Map<String, Set<Aggregator>> inputColumnToAggregators) {
		Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues = new HashMap<>();

		AggregatingMeasurators<Map<String, ?>> coordinatesToAggregates =
				sinkToAggregates(dbQuery, stream, inputColumnToAggregators);

		dbQuery.getAggregators().forEach(aggregator -> {
			AdhocQueryStep adhocStep = AdhocQueryStep.edit(dbQuery).measure(aggregator).build();

			MultiTypeStorage<Map<String, ?>> storage = coordinatesToAggregates.getAggregatorToStorage().get(aggregator);

			if (storage == null) {
				// Typically happens when a filter reject completely one of the underlying measure
				storage = MultiTypeStorage.empty();
			}

			eventBus.post(MeasuratorIsCompleted.builder().measurator(aggregator).nbCells(storage.size()).build());
			log.debug("dbQuery={} generated a column with size={}", dbQuery, storage.size());

			// The aggregation step is done: the storage is supposed not to be edited: we re-use it in place, to
			// spare a copy to an immutable container
			queryStepToValues.put(adhocStep, CoordinatesToValues.builder().storage(storage).build());
		});
		return queryStepToValues;
	}

	private void walkDagUpToQueriedMeasures(DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates,
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
			}

			eventBus.post(QueryStepIsEvaluating.builder().queryStep(queryStep).build());

			IMeasure measure = resolveIfRef(queryStep.getMeasure());

			Set<DefaultEdge> underlyingSteps2 = fromAggregatesToQueried.incomingEdgesOf(queryStep);
			List<AdhocQueryStep> underlyingSteps3 = underlyingSteps2.stream()
					.map(edge -> Graphs.getOppositeVertex(fromAggregatesToQueried, edge, queryStep))
					.collect(Collectors.toList());

			Map<String, AdhocQueryStep> underlyingToStep = new HashMap<>();
			underlyingSteps3.forEach(step -> {
				underlyingToStep.put(resolveIfRef(step.getMeasure()).getName(), step);
			});

			if (measure instanceof IHasUnderlyingMeasures combinator) {
				List<CoordinatesToValues> underlyings =
						combinator.getUnderlyingNames().stream().map(name -> underlyingToStep.get(name)).map(step -> {
							CoordinatesToValues values = queryStepToValues.get(step);

							if (values == null) {
								throw new IllegalStateException("The DAG missed step=%s".formatted(step));
							}

							return values;
						}).collect(Collectors.toList());

				CoordinatesToValues coordinatesToValues =
						combinator.wrapNode(transformationFactory, queryStep).produceOutputColumn(underlyings);

				eventBus.post(MeasuratorIsCompleted.builder()
						.measurator(measure)
						.nbCells(coordinatesToValues.getStorage().size())
						.build());

				queryStepToValues.put(queryStep, coordinatesToValues);
			} else {
				throw new UnsupportedOperationException("%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
			}
		});
	}

	private IMeasure resolveIfRef(IMeasure measure) {
		if (measure == null) {
			throw new IllegalArgumentException("Null input");
		}

		if (measure instanceof ReferencedMeasure ref) {
			String refName = ref.getRef();
			IMeasure resolved = nameToMeasure.get(refName);

			if (resolved == null) {
				throw new IllegalArgumentException("No measure named: %s".formatted(refName));
			}

			return resolved;
		}
		return measure;
	}

	private AggregatingMeasurators<Map<String, ?>> sinkToAggregates(DatabaseQuery adhocQuery,
			Stream<Map<String, ?>> stream,
			Map<String, Set<Aggregator>> inputColumnToAggregators) {

		AggregatingMeasurators<Map<String, ?>> coordinatesToAgg = new AggregatingMeasurators<>(transformationFactory);

		// Process the underlying stream of data to execute aggregations
		stream.forEach(input -> {
			Optional<Map<String, ?>> optCoordinates = makeCoordinate(adhocQuery, input);

			if (optCoordinates.isEmpty()) {
				// Skip this input as it is incompatible with the groupBy
				return;
			}

			if (!FilterHelpers.match(adhocQuery.getFilter(), input)) {
				return;
			}

			// Iterate either on input map or on requested measures, depending on map sizes
			if (inputColumnToAggregators.size() < input.size()) {
				inputColumnToAggregators.forEach((aggName, aggs) -> {
					if (input.containsKey(aggName)) {
						Object v = input.get(aggName);

						aggs.forEach(agg -> coordinatesToAgg.contribute(agg, optCoordinates.get(), v));
					}
				});
			} else {
				input.forEach((k, v) -> {
					if (inputColumnToAggregators.containsKey(k)) {
						Set<Aggregator> aggs = inputColumnToAggregators.get(k);

						aggs.forEach(agg -> coordinatesToAgg.contribute(agg, optCoordinates.get(), v));
					}
				});
			}
		});

		// inputColumnToAggregators.values().stream().flatMap(c -> c.stream()).forEach(aggregator -> {
		// long size = coordinatesToAgg.size(aggregator);
		//
		// eventBus.post(MeasuratorIsCompleted.builder().measurator(aggregator).nbCells(size).build());
		// });

		return coordinatesToAgg;
	}

	/**
	 * 
	 * @param adhocQuery
	 * @param input
	 * @return the coordinate for given input, or empty if the input is not compatible with given groupBys.
	 */
	private Optional<Map<String, ?>> makeCoordinate(IWhereGroupbyAdhocQuery adhocQuery, Map<String, ?> input) {
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
	 * 
	 * @param adhocQuery
	 * @return the Set of {@link IAdhocQuery} to be executed to an underlying Database to be able to execute the
	 *         {@link DAGForQuery}
	 */
	public Set<DatabaseQuery> prepare(IAdhocQuery adhocQuery) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> directedGraph = makeQueryStepsDag(adhocQuery);

		return queryStepsDagToDbQueries(directedGraph, adhocQuery.isExplain());

	}

	private Set<DatabaseQuery> queryStepsDagToDbQueries(DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> directedGraph,
			boolean explain) {
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
						measurelessToAggregators
								.merge(measureless, Collections.singleton(leafAggregator), DAG::unionSet);
					} else {
						throw new IllegalStateException("Expected simple aggregators. Got %s".formatted(leafMeasure));
					}
				});

		if (explain) {
			new TopologicalOrderIterator<>(directedGraph).forEachRemaining(step -> {
				Set<DefaultEdge> underlyings = directedGraph.outgoingEdgesOf(step);

				underlyings.forEach(edge -> log
						.info("[EXPLAIN] {} -> {}", step, Graphs.getOppositeVertex(directedGraph, edge, step)));
			});
		}

		return measurelessToAggregators.entrySet().stream().map(e -> {
			MeasurelessQuery adhocLeafQuery = e.getKey();
			Set<Aggregator> leafAggregators = e.getValue();
			return new DatabaseQuery(adhocLeafQuery.getFilter(), adhocLeafQuery.getGroupBy(), leafAggregators);
		}).collect(Collectors.toSet());
	}

	private DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> makeQueryStepsDag(IAdhocQuery adhocQuery) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> queryDag = new DirectedAcyclicGraph<>(DefaultEdge.class);

		LinkedList<AdhocQueryStep> collectors = new LinkedList<>();

		{

			adhocQuery.getMeasures().stream().map(ref -> resolveIfRef(ref)).forEach(queriedMeasure -> {
				AdhocQueryStep rootStep = AdhocQueryStep.builder()
						.filter(adhocQuery.getFilter())
						.groupBy(adhocQuery.getGroupBy())
						.measure(queriedMeasure)
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

				for (AdhocQueryStep underlyingStep : combinator.wrapNode(transformationFactory, adhocSubQuery)
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

	public ITabularView execute(IAdhocQuery adhocQuery, IAdhocDatabase db) {
		return execute(adhocQuery, Set.of(), db);
	}

	public ITabularView execute(IAdhocQuery adhocQuery, Set<? extends IQueryOption> queryOptions, IAdhocDatabase db) {
		Set<DatabaseQuery> prepared = prepare(adhocQuery);

		Map<DatabaseQuery, Stream<Map<String, ?>>> dbQueryToStream = new HashMap<>();
		for (DatabaseQuery dbQuery : prepared) {
			dbQueryToStream.put(dbQuery, db.openDbStream(dbQuery));
		}

		return execute(adhocQuery, queryOptions, dbQueryToStream);
	}

}
