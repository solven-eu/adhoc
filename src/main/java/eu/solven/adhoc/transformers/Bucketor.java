package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ITransformationFactory;
import eu.solven.adhoc.aggregations.SumAggregator;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
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
 *
 */
@Value
@Builder
@Slf4j
public class Bucketor implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@NonNull
	String underlyingMeasure;

	@NonNull
	@Default
	String aggregationKey = SumAggregator.KEY;

	@NonNull
	IAdhocGroupBy groupBy;

	@Override
	public List<String> getUnderlyingMeasures() {
		return Collections.singletonList(underlyingMeasure);
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps(AdhocQueryStep step) {
		AdhocQueryStep underlyingStep = AdhocQueryStep.edit(step)
				.groupBy(GroupByHelpers.union(step.getGroupBy(), groupBy))
				.measure(ReferencedMeasure.builder().ref(underlyingMeasure).build())
				.build();
		return Collections.singletonList(underlyingStep);
	}

	@Override
	public CoordinatesToValues produceOutputColumn(ITransformationFactory transformationFactory,
			AdhocQueryStep queryStep,
			List<CoordinatesToValues> underlyings) {
		if (underlyings.size() != 1) {
			throw new IllegalArgumentException("underlyings.size() != 1");
		} else if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		MultiTypeStorage<Map<String, ?>> aggregatingView = MultiTypeStorage.<Map<String, ?>>builder().build();

		IAggregation agg = transformationFactory.makeAggregation(aggregationKey);

		for (Map<String, ?> coordinate : keySet(underlyings)) {
			List<Object> underlyingVs = underlyings.stream().map(storage -> {
				AtomicReference<Object> refV = new AtomicReference<>();
				AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
					refV.set(o);
				});

				storage.onValue(coordinate, consumer);

				return refV.get();
			}).collect(Collectors.toList());

			Object value = underlyingVs.get(0);

			if (value != null) {
				Map<String, ?> outputCoordinate = queryGroupBy(queryStep.getGroupBy(), coordinate);
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

	private Iterable<Map<String, ?>> keySet(List<CoordinatesToValues> underlyings) {
		Set<Map<String, ?>> keySet = new HashSet<>();

		for (CoordinatesToValues underlying : underlyings) {
			keySet.addAll(underlying.getStorage().keySet());
		}

		return keySet;
	}

}
