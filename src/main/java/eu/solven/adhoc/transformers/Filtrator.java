package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.ITransformationFactory;
import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Slf4j
public class Filtrator implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@NonNull
	String underlyingMeasure;

	@NonNull
	IAdhocFilter filter;

	@Override
	public List<String> getUnderlyingMeasures() {
		return Collections.singletonList(underlyingMeasure);
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps(AdhocQueryStep step) {
		AdhocQueryStep underlyingStep = AdhocQueryStep.edit(step)
				.filter(AndFilter.and(step.getFilter(), filter))
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

		CoordinatesToValues output = CoordinatesToValues.builder().build();

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
			output.put(coordinate, value);
		}

		return output;
	}

	private Iterable<Map<String, ?>> keySet(List<CoordinatesToValues> underlyings) {

		Set<Map<String, ?>> keySet = new HashSet<>();

		for (CoordinatesToValues underlying : underlyings) {
			keySet.addAll(underlying.getStorage().keySet());
		}

		return keySet;
	}

}
