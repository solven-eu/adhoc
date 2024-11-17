package eu.solven.adhoc.transformers;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.coordinate.MapComparators;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.execute.GroupByHelpers;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class BucketorQueryStep implements IHasUnderlyingQuerySteps {
	final Bucketor bucketor;
	final IOperatorsFactory transformationFactory;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return bucketor.getUnderlyingNames();
	};

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		return getUnderlyingNames().stream().map(underlying -> {
			AdhocQueryStep object = AdhocQueryStep.edit(step)
					.groupBy(GroupByHelpers.union(step.getGroupBy(), bucketor.getGroupBy()))
					.measure(ReferencedMeasure.builder().ref(underlying).build())
					.build();
			return object;
		}).collect(Collectors.toList());
	}

	@Override
	public CoordinatesToValues produceOutputColumn(List<CoordinatesToValues> underlyings) {
		if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		MultiTypeStorage<Map<String, ?>> aggregatingView = MultiTypeStorage.<Map<String, ?>>builder().build();

		IAggregation agg = transformationFactory.makeAggregation(bucketor.getAggregationKey());
		ICombination combinator =
				transformationFactory.makeCombination(bucketor.getCombinatorKey(), getCombinatorOptions());

		List<String> underlyingNames = getUnderlyingNames();

		for (Map<String, ?> coordinate : keySet(bucketor.isDebug(), underlyings)) {
			List<Object> underlyingVs = underlyings.stream().map(storage -> {
				AtomicReference<Object> refV = new AtomicReference<>();
				AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
					refV.set(o);
				});

				storage.onValue(coordinate, consumer);

				return refV.get();
			}).collect(Collectors.toList());

			Object value = combinator.combine(underlyingVs);

			if (bucketor.isDebug()) {
				Map<String, Object> underylingVsAsMap = new TreeMap<>();

				for (int i = 0; i < underlyingNames.size(); i++) {
					underylingVsAsMap.put(underlyingNames.get(i), underlyingVs.get(i));
				}

				log.info("Combinator={} transformed {} into {} at {}",
						bucketor.getCombinatorKey(),
						underylingVsAsMap,
						value,
						coordinate);
			}

			if (value != null) {
				Map<String, ?> outputCoordinate = queryGroupBy(step.getGroupBy(), coordinate);

				if (bucketor.isDebug()) {
					log.info("Contribute {} into {}", value, outputCoordinate);
				}

				aggregatingView.merge(outputCoordinate, value, agg);
			}
		}

		return CoordinatesToValues.builder().storage(aggregatingView).build();
	}

	private Map<String, ?> getCombinatorOptions() {
		return Combinator.makeAllOptions(bucketor, bucketor.getCombinatorOptions());
	}

	private Map<String, ?> queryGroupBy(IAdhocGroupBy queryGroupBy, Map<String, ?> coordinates) {
		Map<String, Object> queryCoordinates = new HashMap<>();

		queryGroupBy.getGroupedByColumns().forEach(groupBy -> {
			Object value = coordinates.get(groupBy);

			if (value == null) {
				// Should we accept null a coordinate, e.g. to handle input partial Maps?
				throw new IllegalStateException("A coordinate-value can not be null");
			}

			queryCoordinates.put(groupBy, value);
		});

		return queryCoordinates;
	}

	public static Iterable<? extends Map<String, ?>> keySet(boolean debug, List<CoordinatesToValues> underlyings) {
		Set<Map<String, ?>> keySet = newSet(debug);

		for (CoordinatesToValues underlying : underlyings) {
			keySet.addAll(underlying.getStorage().keySet());
		}

		return keySet;
	}

	public static Set<Map<String, ?>> newSet(boolean debug) {
		Set<Map<String, ?>> keySet;
		if (debug) {
			// Enforce an iteration order for debugging-purposes
			Comparator<Map<String, ?>> mapComparator = MapComparators.mapComparator();
			keySet = new TreeSet<>(mapComparator);
		} else {
			keySet = new HashSet<>();
		}
		return keySet;
	}
}
