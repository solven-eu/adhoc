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
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.greenrobot.eventbus.EventBus;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ITransformation;
import eu.solven.adhoc.aggregations.MaxAggregator;
import eu.solven.adhoc.aggregations.MaxTransformation;
import eu.solven.adhoc.aggregations.SumAggregator;
import eu.solven.adhoc.aggregations.SumTransformation;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.eventbus.MeasuratorIsCompleted;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.AdhocQueryBuilder;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.storage.AggregatingMeasurators;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.CollectingCombinators;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DAG {
	final EventBus eventBus;

	final Map<String, IMeasure> nameToMeasurator = new HashMap<>();

	public void addMeasure(IMeasure namedMeasure) {
		nameToMeasurator.put(namedMeasure.getName(), namedMeasure);
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
		DAGForQuery dagForQuery = prepareDAGForQuery(
				adhocQuery.getMeasures().stream().map(ReferencedMeasure::getRef).collect(Collectors.toSet()));

		Map<String, List<Aggregator>> inputColumnToAggregators = new LinkedHashMap<>();

		dagForQuery.getImpliedAggregators().stream().forEach(aggregator -> {
			inputColumnToAggregators.merge(aggregator.getName(), List.of(aggregator), DAG::unionList);
		});

		AggregatingMeasurators aggregatingMeasurators = new AggregatingMeasurators();

		// Process the underlying stream of data to execute aggregations
		stream.forEach(input -> {
			// Iterate either on input map or on requested measures, depending on map sizes
			if (inputColumnToAggregators.size() < input.size()) {
				inputColumnToAggregators.forEach((aggName, aggs) -> {
					if (input.containsKey(aggName)) {
						Object v = input.get(aggName);

						aggs.forEach(agg -> aggregatingMeasurators.contribute(agg, v));
					}
				});
			} else {
				input.forEach((k, v) -> {
					if (inputColumnToAggregators.containsKey(k)) {
						List<Aggregator> aggs = inputColumnToAggregators.get(k);

						aggs.forEach(agg -> aggregatingMeasurators.contribute(agg, v));
					}
				});
			}
		});

		inputColumnToAggregators.values().stream().flatMap(c -> c.stream()).forEach(aggregator -> {
			eventBus.post(MeasuratorIsCompleted.builder()
					.measurator(aggregator)
					.nbCells(aggregatingMeasurators.size(aggregator))
					.build());
		});

		// https://stackoverflow.com/questions/24511052/how-to-convert-an-iterator-to-a-stream
		List<Combinator> depthFirstMeasures = StreamSupport
				.stream(Spliterators.spliteratorUnknownSize(dagForQuery.getBreadthFirst(), Spliterator.ORDERED), false)
				.filter(Combinator.class::isInstance)
				.map(Combinator.class::cast)
				.collect(Collectors.toList());

		// We want to traverse the tree from pre-aggregated measures to requested measures
		Collections.reverse(depthFirstMeasures);

		Set<IMeasure> queryMeasurators = dagForQuery.getQueryMeasurators();

		CollectingCombinators collectingCombinators = new CollectingCombinators();

		depthFirstMeasures.forEach(computedCombinator -> {
			List<String> underlyingMs = computedCombinator.getUnderlyingMeasurators();

			List<Object> underlyingVs = underlyingMs.stream().map(underlyingM -> {
				AtomicReference<Object> refV = new AtomicReference<>();
				AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
					refV.set(o);
				});

				IMeasure measurator = nameToMeasurator.get(underlyingM);
				if (measurator instanceof Aggregator aggregator) {
					aggregatingMeasurators.onValue(aggregator, consumer);
				} else if (measurator instanceof Combinator combinator) {
					collectingCombinators.onValue(combinator, consumer);
				} else {
					throw new UnsupportedOperationException(measurator.getClass().getName());
				}

				return refV.get();
			}).collect(Collectors.toList());

			Object combined = makeCombinator(computedCombinator).transform(underlyingVs);

			// long size;
			if (combined != null) {
				collectingCombinators.contribute(computedCombinator, combined);
				// size = 1;
				// } else {
				// size = 0;
			}

			eventBus.post(MeasuratorIsCompleted.builder()
					.measurator(computedCombinator)
					.nbCells(collectingCombinators.size(computedCombinator))
					.build());

		});

		MapBasedTabularView mapBasedTabularView = new MapBasedTabularView();

		Iterator<IMeasure> measuresToReturn;
		if (queryOptions.contains(StandardQueryOptions.RETURN_UNDERLYING_MEASURES)) {
			measuresToReturn = dagForQuery.getBreadthFirst();
		} else {
			measuresToReturn = queryMeasurators.iterator();
		}

		measuresToReturn.forEachRemaining(measurator -> {
			AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
				mapBasedTabularView.append(Collections.emptyMap(), Map.of(measurator.getName(), o));
			});
			if (measurator instanceof Aggregator aggregator) {
				aggregatingMeasurators.onValue(aggregator, consumer);
			} else if (measurator instanceof Combinator combinator) {
				collectingCombinators.onValue(combinator, consumer);
			} else {
				throw new UnsupportedOperationException(measurator.getClass().getName());
			}
		});

		return mapBasedTabularView;
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

	private DAGForQuery prepareDAGForQuery(Set<String> measures) {
		DirectedAcyclicGraph<IMeasure, DefaultEdge> directedGraph = new DirectedAcyclicGraph<>(DefaultEdge.class);

		nameToMeasurator.forEach((name, measurator) -> {
			directedGraph.addVertex(measurator);
			if (measurator instanceof Combinator combinator) {
				List<IMeasure> namedUnderlyings = combinator.getUnderlyingMeasurators()
						.stream()
						.map(nameToMeasurator::get)
						.collect(Collectors.toList());

				namedUnderlyings.forEach(underlying -> {
					directedGraph.addEdge(measurator, underlying);
				});

			} else if (measurator instanceof Aggregator) {
				log.debug("Aggregator has no underlyings");
			} else {
				throw new IllegalArgumentException("Arg on %s".formatted(measurator));
			}
		});

		DAGForQuery dagForQuery = DAGForQuery.builder().directedGraph(directedGraph).queriedMeasures(measures).build();
		return dagForQuery;
	}

	/**
	 * 
	 * @param adhocQuery
	 * @return the Set of {@link IAdhocQuery} to be executed to an underlying Database to be able to execute the
	 *         {@link DAGForQuery}
	 */
	public Set<DatabaseQuery> prepare(IAdhocQuery adhocQuery) {
		// DAGForQuery dagForQuery = prepareDAGForQuery(
		// adhocQuery.getMeasures().stream().map(ReferencedMeasure::getRef).collect(Collectors.toSet()));

		// Aggregators expressed the actual query to the underlying DB
		LinkedList<IAdhocQuery> aggregators = new LinkedList<>();
		// Collectors are intermediate measures in the DAG
		LinkedList<IAdhocQuery> collectors = new LinkedList<>();

		collectors.add(adhocQuery);

		while (!collectors.isEmpty()) {
			IAdhocQuery adhocSubQuery = collectors.poll();

			for (ReferencedMeasure underlyingMeasure : adhocSubQuery.getMeasures()) {
				String ref = underlyingMeasure.getRef();

				IMeasure measure = nameToMeasurator.get(ref);

				if (measure instanceof Aggregator aggregator) {
					AdhocQuery subQueryToUnderlyings = AdhocQueryBuilder.edit(adhocSubQuery)
							.clearMeasures()
							.addMeasures(aggregator.getName())
							.build();

					aggregators.add(subQueryToUnderlyings);
				} else if (measure instanceof Combinator combinator) {

					AdhocQuery subQueryToUnderlyings = AdhocQueryBuilder.edit(adhocSubQuery)
							.clearMeasures()
							.addMeasures(combinator.getUnderlyingMeasurators())
							.build();

					collectors.add(subQueryToUnderlyings);
				} else {
					throw new UnsupportedOperationException(measure.toString());
				}
			}
		}

		Map<AdhocQuery, Set<Aggregator>> measurelessToAggregators = new HashMap<>();

		for (IAdhocQuery leafQuery : aggregators) {
			if (leafQuery.getMeasures().size() != 1) {
				throw new IllegalStateException("Expected simple aggregators");
			}
			ReferencedMeasure leafRefMeasure = leafQuery.getMeasures().iterator().next();
			IMeasure leafMeasure = nameToMeasurator.get(leafRefMeasure.getRef());

			if (leafMeasure instanceof Aggregator leafAggregator) {
				AdhocQuery measureless = AdhocQueryBuilder.edit(leafQuery).clearMeasures().build();

				// We could analyze filters, to discard a query filter `k=v` if another query filters `k=v|v2`
				measurelessToAggregators.merge(measureless, Collections.singleton(leafAggregator), DAG::unionSet);
			} else {
				throw new IllegalStateException("Expected simple aggregators. Got %s".formatted(leafMeasure));
			}
		}

		return measurelessToAggregators.entrySet().stream().map(e -> {
			AdhocQuery adhocLeafQuery = e.getKey();
			Set<Aggregator> leafAggregators = e.getValue();
			return new DatabaseQuery(adhocLeafQuery::getFilters, adhocLeafQuery::getGroupBys, leafAggregators);
		}).collect(Collectors.toSet());

	}

}
