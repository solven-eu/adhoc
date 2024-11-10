package eu.solven.adhoc.transformers;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.ITransformation;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.dag.DAG;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Slf4j
public class Combinator implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@NonNull
	List<String> underlyingMeasures;

	@NonNull
	String transformationKey;

	public List<AdhocQueryStep> getUnderlyingSteps(AdhocQueryStep adhocSubQuery) {
		return getUnderlyingMeasures().stream().map(underlyingName -> {
			return AdhocQueryStep.builder()
					.filter(adhocSubQuery.getFilter())
					.groupBy(adhocSubQuery.getGroupBy())
					.measure(ReferencedMeasure.builder().ref(underlyingName).build())
					.build();
		}).toList();
	}

	public CoordinatesToValues produceOutputColumn(List<CoordinatesToValues> underlyings) {
		if (underlyings.size() != underlyingMeasures.size()) {
			throw new IllegalArgumentException("underlyingMeasures.size() != underlyings.size()");
		} else if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		CoordinatesToValues output = CoordinatesToValues.builder().build();

		Set<Map<String, ?>> coordinatesManaged = new HashSet<>();
		
		ITransformation tranformation = DAG.makeCombinator(this);

		for (Map<String, ?> coordinate : underlyings.get(0).getStorage().keySet()) {
			if (!coordinatesManaged.add(coordinate)) {
				log.trace("{} has already been handled", coordinate);
			} else {
				List<Object> underlyingVs = underlyings.stream().map(storage -> {
					AtomicReference<Object> refV = new AtomicReference<>();
					AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
						refV.set(o);
					});

					storage.onValue(coordinate, consumer);

					return refV.get();
				}).collect(Collectors.toList());
				
				Object value = tranformation.transform(underlyingVs);
				output.put(coordinate, value);
			}
		}

		return output;
	}

}
