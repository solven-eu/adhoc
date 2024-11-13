package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ITransformation;
import eu.solven.adhoc.aggregations.ITransformationFactory;
import eu.solven.adhoc.aggregations.SumAggregator;
import eu.solven.adhoc.aggregations.SumTransformation;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.coordinate.NavigableMapComparator;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.execute.GroupByHelpers;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link IMeasure} will aggregate underlying measure, evaluated at buckets defined by a {@link IAdhocGroupBy}, and
 * aggregated through an {@link IAggregation}.
 * 
 * @author Benoit Lacelle
 * @see SingleBucketor
 */
@Value
@Builder
@Slf4j
public class Bucketor implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@NonNull
	List<String> underlyingMeasures;

	@NonNull
	@Default
	String aggregationKey = SumAggregator.KEY;

	// Accept a combinator key, to be applied on each groupBy
	@NonNull
	@Default
	String combinatorKey = SumTransformation.KEY;

	@NonNull
	@Default
	Map<String, ?> combinatorOptions = Collections.emptyMap();

	@NonNull
	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	@Default
	boolean debug = false;

	@Override
	public List<String> getUnderlyingNames() {
		return underlyingMeasures;
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps(AdhocQueryStep step) {
		return getUnderlyingNames().stream().map(underlying -> {
			return AdhocQueryStep.edit(step)
					.groupBy(GroupByHelpers.union(step.getGroupBy(), groupBy))
					.measure(ReferencedMeasure.builder().ref(underlying).build())
					.build();
		}).collect(Collectors.toList());
	}

	@Override
	public CoordinatesToValues produceOutputColumn(ITransformationFactory transformationFactory,
			AdhocQueryStep queryStep,
			List<CoordinatesToValues> underlyings) {
		if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		MultiTypeStorage<Map<String, ?>> aggregatingView = MultiTypeStorage.<Map<String, ?>>builder().build();

		IAggregation agg = transformationFactory.makeAggregation(aggregationKey);
		ITransformation combinator =
				transformationFactory.makeTransformation(getCombinatorKey(), getCombinatorOptions());

		for (Map<String, ?> coordinate : keySet(underlyings)) {
			List<Object> underlyingVs = underlyings.stream().map(storage -> {
				AtomicReference<Object> refV = new AtomicReference<>();
				AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
					refV.set(o);
				});

				storage.onValue(coordinate, consumer);

				return refV.get();
			}).collect(Collectors.toList());

			Object value = combinator.transform(underlyingVs);

			if (debug) {
				Map<String, Object> underylingVsAsMap = new TreeMap<>();

				for (int i = 0; i < underlyingMeasures.size(); i++) {
					underylingVsAsMap.put(underlyingMeasures.get(i), underlyingVs.get(i));
				}

				log.info("Combinator={} transformed {} into {} at {}",
						getCombinatorKey(),
						underylingVsAsMap,
						value,
						coordinate);
			}

			if (value != null) {
				Map<String, ?> outputCoordinate = queryGroupBy(queryStep.getGroupBy(), coordinate);

				if (debug) {
					log.info("Contribute {} into {}", value, outputCoordinate);
				}

				aggregatingView.merge(outputCoordinate, value, agg);
			}
		}

		return CoordinatesToValues.builder().storage(aggregatingView).build();

		// Transfer to a flat-table
		// CoordinatesToValues output = CoordinatesToValues.builder().build();
		// aggregatingView.scan(new RowScanner<Map<String, ?>>() {
		//
		// @Override
		// public ValueConsumer onKey(Map<String, ?> coordinates) {
		// return AsObjectValueConsumer.consumer(o -> {
		// output.put(coordinates, o);
		// });
		// }
		// });
		//
		// return output;
	}

	private Map<String, ?> getCombinatorOptions() {
		return Combinator.makeAllOptions(this, combinatorOptions);
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

	private Iterable<? extends Map<String, ?>> keySet(List<CoordinatesToValues> underlyings) {
		Set<Map<String, ?>> keySet;
		if (debug) {
			// Enforce an iteration order for debugging-purposes
			Comparator<Map<String, ?>> mapComparator =
					Comparator.<Map<String, ?>, NavigableMap<String, ?>>comparing(s -> new TreeMap<String, Object>(s),
							NavigableMapComparator.INSTANCE);
			keySet = new TreeSet<>(mapComparator);
		} else {
			keySet = new HashSet<>();
		}

		for (CoordinatesToValues underlying : underlyings) {
			keySet.addAll(underlying.getStorage().keySet());
		}

		return keySet;
	}

}
