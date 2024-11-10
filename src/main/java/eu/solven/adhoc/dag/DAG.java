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

import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.RowScanner;
import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ITransformation;
import eu.solven.adhoc.aggregations.MaxAggregator;
import eu.solven.adhoc.aggregations.MaxTransformation;
import eu.solven.adhoc.aggregations.SumAggregator;
import eu.solven.adhoc.aggregations.SumTransformation;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.MeasuratorIsCompleted;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.storage.AggregatingMeasurators2;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import eu.solven.adhoc.storage.ValueConsumer;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import eu.solven.pepper.logging.PepperLogHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DAG {
	final EventBus eventBus;

	final Map<String, IMeasure> nameToMeasure = new HashMap<>();

	public void addMeasure(IMeasure namedMeasure) {
		nameToMeasure.put(namedMeasure.getName(), namedMeasure);
	}

	public ITabularView execute(IAdhocQuery adhocQuery, Stream<Map<String, ?>> stream) {
		return execute(adhocQuery, Set.of(), stream);
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

	public ITabularView execute(IAdhocQuery adhocQuery,
			Set<? extends IQueryOption> queryOptions,
			Stream<Map<String, ?>> stream) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates = makeQueryStepsDag(adhocQuery);

		Map<String, List<Aggregator>> inputColumnToAggregators = new LinkedHashMap<>();

		fromQueriedToAggregates.vertexSet()
				.stream()
				.filter(step -> fromQueriedToAggregates.outgoingEdgesOf(step).size() == 0)
				.map(step -> step.getMeasure())
				.forEach(measure -> {
					measure = resolveIfRef(measure);

					if (measure instanceof Aggregator aggregator) {
						inputColumnToAggregators.merge(aggregator.getName(), List.of(aggregator), DAG::unionList);
					} else {
						throw new UnsupportedOperationException(
								"%s".formatted(PepperLogHelper.getObjectAndClass(measure)));
					}
				});

		Set<DatabaseQuery> dbQueries = queryStepsDagToDbQueries(fromQueriedToAggregates);
		if (dbQueries.isEmpty()) {
			return ITabularView.empty();
		} else if (dbQueries.size() >= 2) {
			throw new UnsupportedOperationException("Limitation. Open a GithubTicket.");
		}

		Map<DatabaseQuery, Stream<Map<String, ?>>> dbQueryToSteam = new HashMap<>();
		dbQueryToSteam.put(dbQueries.iterator().next(), stream);

		Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues =
				aggregateStreamToAggregates(stream, inputColumnToAggregators, dbQueryToSteam);

		// We're done with the input stream: the DB can be shutdown, we could answer the query
		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("aggregates").build());

		walkDagUpToQueriedMeasures(fromQueriedToAggregates, queryStepToValues);

		eventBus.post(AdhocQueryPhaseIsCompleted.builder().phase("transformations").build());

		MapBasedTabularView mapBasedTabularView =
				toTabularView(queryOptions, fromQueriedToAggregates, queryStepToValues);

		return mapBasedTabularView;
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
				public void onRow(Map<String, ?> coordinates, Map<String, ?> values) {
					// TODO Auto-generated method stub

				}

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

	private Map<AdhocQueryStep, CoordinatesToValues> aggregateStreamToAggregates(Stream<Map<String, ?>> stream,
			Map<String, List<Aggregator>> inputColumnToAggregators,
			Map<DatabaseQuery, Stream<Map<String, ?>>> dbQueryToSteam) {
		Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues = new HashMap<>();

		// This is the only step consuming the input stream
		dbQueryToSteam.forEach((dbQuery, dbStream) -> {
			AggregatingMeasurators2<Map<String, ?>> coordinatesToAggregates =
					sinkToAggregates(dbQuery, stream, inputColumnToAggregators);

			dbQuery.getAggregators().forEach(aggregator -> {
				AdhocQueryStep adhocStep = AdhocQueryStep.edit(dbQuery).measure(aggregator).build();

				MultiTypeStorage<Map<String, ?>> storage =
						coordinatesToAggregates.getAggregatorToStorage().get(aggregator);

				// The aggregation step is done: the storage is supposed not to be edited: we re-use it in place, to
				// spare a copy to an immutable container
				queryStepToValues.put(adhocStep, CoordinatesToValues.builder().storage(storage).build());
			});
		});
		return queryStepToValues;
	}

	private void walkDagUpToQueriedMeasures(DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates,
			Map<AdhocQueryStep, CoordinatesToValues> queryStepToValues) {
		EdgeReversedGraph<AdhocQueryStep, DefaultEdge> fromAggregatesToQueried =
				new EdgeReversedGraph<>(fromQueriedToAggregates);

		new BreadthFirstIterator<>(fromAggregatesToQueried).forEachRemaining(queryStep -> {

			if (queryStepToValues.containsKey(queryStep)) {
				// This typically happens on aggregator measures, as they are fed in a previous step
				return;
			}

			IMeasure measure = resolveIfRef(queryStep.getMeasure());

			Set<DefaultEdge> underlyingSteps2 = fromAggregatesToQueried.incomingEdgesOf(queryStep);
			List<AdhocQueryStep> underlyingSteps3 = underlyingSteps2.stream()
					.map(edge -> Graphs.getOppositeVertex(fromAggregatesToQueried, edge, queryStep))
					.collect(Collectors.toList());

			Map<String, AdhocQueryStep> underlyingToStep = new HashMap<>();
			underlyingSteps3.forEach(step -> {
				underlyingToStep.put(resolveIfRef(step.getMeasure()).getName(), step);
			});

			if (measure instanceof Combinator combinator) {
				queryStepToValues.get(underlyingToStep.get(combinator.getUnderlyingMeasures().get(0)));

				List<CoordinatesToValues> underlyings = combinator.getUnderlyingMeasures()
						.stream()
						.map(name -> underlyingToStep.get(name))
						.map(step -> queryStepToValues.get(step))
						.collect(Collectors.toList());

				if (underlyings.contains(null)) {
					throw new IllegalStateException("The DAG missed one step");
				}

				CoordinatesToValues coordinatesToValues = combinator.produceOutputColumn(underlyings);
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

	private AggregatingMeasurators2<Map<String, ?>> sinkToAggregates(DatabaseQuery adhocQuery,
			Stream<Map<String, ?>> stream,
			Map<String, List<Aggregator>> inputColumnToAggregators) {

		AggregatingMeasurators2<Map<String, ?>> coordinatesToAgg = new AggregatingMeasurators2<>();

		// Process the underlying stream of data to execute aggregations
		stream.forEach(input -> {
			Optional<Map<String, ?>> optCoordinates = makeCoordinate(adhocQuery, input);

			if (optCoordinates.isEmpty()) {
				// Skip this input as it is incompatible with the groupBy
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
						List<Aggregator> aggs = inputColumnToAggregators.get(k);

						aggs.forEach(agg -> coordinatesToAgg.contribute(agg, optCoordinates.get(), v));
					}
				});
			}
		});

		inputColumnToAggregators.values().stream().flatMap(c -> c.stream()).forEach(aggregator -> {
			long size = coordinatesToAgg.size(aggregator);

			eventBus.post(MeasuratorIsCompleted.builder().measurator(aggregator).nbCells(size).build());
		});

		return coordinatesToAgg;
	}

	// private List<Combinator> fromLeafToQueried(DAGForQuery dagForQuery) {
	// // https://stackoverflow.com/questions/24511052/how-to-convert-an-iterator-to-a-stream
	// List<Combinator> fromLeafToQueried = StreamSupport
	// .stream(Spliterators.spliteratorUnknownSize(dagForQuery.getBreadthFirst(), Spliterator.ORDERED), false)
	// // Skip aggregators as they are evaluated in a separate phase
	// .filter(Combinator.class::isInstance)
	// .map(Combinator.class::cast)
	// .collect(Collectors.toList());
	//
	// // We want to traverse the tree from pre-aggregated measures to requested measures
	// Collections.reverse(fromLeafToQueried);
	// return fromLeafToQueried;
	// }

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

	public static ITransformation makeCombinator(Combinator combinator) {
		String transformationKey = combinator.getTransformationKey();
		return switch (transformationKey) {
		case SumTransformation.KEY: {
			yield new SumTransformation();
		}
		case MaxTransformation.KEY: {
			yield new MaxTransformation();
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + transformationKey);
		};
	}

	public static IAggregation makeAggregation(String aggregationKey) {
		return switch (aggregationKey) {
		case SumAggregator.KEY: {
			yield new SumAggregator();
		}
		case MaxAggregator.KEY: {
			yield new MaxAggregator();
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + aggregationKey);
		};
	}

	// This is the DAG of measures relationships, independently of filters and groupBys
	// private DAGForQuery prepareDAGForQuery(Set<String> measures) {
	// DirectedAcyclicGraph<IMeasure, DefaultEdge> directedGraph = new DirectedAcyclicGraph<>(DefaultEdge.class);
	//
	// nameToMeasure.forEach((name, measurator) -> {
	// directedGraph.addVertex(measurator);
	// if (measurator instanceof Combinator combinator) {
	// List<IMeasure> namedUnderlyings = combinator.getUnderlyingMeasures()
	// .stream()
	// .map(nameToMeasure::get)
	// .collect(Collectors.toList());
	//
	// namedUnderlyings.forEach(underlying -> {
	// directedGraph.addEdge(measurator, underlying);
	// });
	//
	// } else if (measurator instanceof Aggregator) {
	// log.debug("Aggregator has no underlyings");
	// } else {
	// throw new IllegalArgumentException("Arg on %s".formatted(measurator));
	// }
	// });
	//
	// DAGForQuery dagForQuery = DAGForQuery.builder().directedGraph(directedGraph).queriedMeasures(measures).build();
	// return dagForQuery;
	// }

	/**
	 * 
	 * @param adhocQuery
	 * @return the Set of {@link IAdhocQuery} to be executed to an underlying Database to be able to execute the
	 *         {@link DAGForQuery}
	 */
	public Set<DatabaseQuery> prepare(IAdhocQuery adhocQuery) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> directedGraph = makeQueryStepsDag(adhocQuery);

		return queryStepsDagToDbQueries(directedGraph);

	}

	private Set<DatabaseQuery> queryStepsDagToDbQueries(
			DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> directedGraph) {
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

		return measurelessToAggregators.entrySet().stream().map(e -> {
			MeasurelessQuery adhocLeafQuery = e.getKey();
			Set<Aggregator> leafAggregators = e.getValue();
			return new DatabaseQuery(adhocLeafQuery.getFilter(), adhocLeafQuery.getGroupBy(), leafAggregators);
		}).collect(Collectors.toSet());
	}

	private DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> makeQueryStepsDag(IAdhocQuery adhocQuery) {
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> queryDag = new DirectedAcyclicGraph<>(DefaultEdge.class);

		// Aggregators expressed the actual query to the underlying DB
		// LinkedList<AdhocQueryStep> aggregators = new LinkedList<>();
		// Collectors are intermediate measures in the DAG
		LinkedList<AdhocQueryStep> collectors = new LinkedList<>();

		{

			adhocQuery.getMeasures().stream().map(ref -> resolveIfRef(ref)).forEach(queriedMeasure -> {
				AdhocQueryStep rootQueryStep = AdhocQueryStep.builder()
						.filter(adhocQuery.getFilter())
						.groupBy(adhocQuery.getGroupBy())
						.measure(queriedMeasure)
						.build();

				queryDag.addVertex(rootQueryStep);
				collectors.add(rootQueryStep);
			});

		}
		// directedGraph.vertexSet()
		// .stream()
		// .filter(step -> directedGraph.outgoingEdgesOf(step).size() == 0).collect(Collectors)

		while (!collectors.isEmpty()) {
			AdhocQueryStep adhocSubQuery = collectors.poll();

			IMeasure measure = resolveIfRef(adhocSubQuery.getMeasure());

			if (measure instanceof Aggregator aggregator) {
				// aggregators.add(adhocSubQuery);
			} else if (measure instanceof Combinator combinator) {

				for (AdhocQueryStep subQueryToUnderlying : combinator.getUnderlyingSteps(adhocSubQuery)) {
					// Make sure the DAG has actual measure nodes, and not references
					IMeasure notRefMeasure = resolveIfRef(subQueryToUnderlying.getMeasure());
					subQueryToUnderlying = AdhocQueryStep.edit(subQueryToUnderlying).measure(notRefMeasure).build();

					queryDag.addVertex(subQueryToUnderlying);
					queryDag.addEdge(adhocSubQuery, subQueryToUnderlying);

					collectors.add(subQueryToUnderlying);
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
