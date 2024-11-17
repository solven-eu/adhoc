package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class FiltratorQueryStep implements IHasUnderlyingQuerySteps {
	final Filtrator filtrator;
	final IOperatorsFactory transformationFactory;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return filtrator.getUnderlyingNames();
	};

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		AdhocQueryStep underlyingStep = AdhocQueryStep.edit(step)
				.filter(AndFilter.and(step.getFilter(), filtrator.getFilter()))
				.measure(ReferencedMeasure.builder().ref(filtrator.getUnderlyingMeasure()).build())
				.build();
		return Collections.singletonList(underlyingStep);
	}

	@Override
	public CoordinatesToValues produceOutputColumn(List<CoordinatesToValues> underlyings) {
		if (underlyings.size() != 1) {
			throw new IllegalArgumentException("underlyings.size() != 1");
		} else if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		CoordinatesToValues output = CoordinatesToValues.builder().build();

		for (Map<String, ?> coordinate : BucketorQueryStep.keySet(filtrator.isDebug(), underlyings)) {
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
}
