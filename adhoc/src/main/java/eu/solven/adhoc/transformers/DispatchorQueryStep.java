package eu.solven.adhoc.transformers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.IDecomposition;
import eu.solven.adhoc.aggregations.ITransformationFactory;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DispatchorQueryStep implements IHasUnderlyingQuerySteps {
	final Dispatchor dispatchor;
	final ITransformationFactory transformationFactory;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return dispatchor.getUnderlyingNames();
	};

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		IDecomposition decomposition = makeDecomposition();

		return decomposition.getUnderlyingSteps(step).stream().map(subStep -> {
			return AdhocQueryStep.edit(subStep)
					.measure(ReferencedMeasure.builder().ref(dispatchor.getUnderlyingMeasure()).build())
					.build();
		}).collect(Collectors.toList());

	}

	private IDecomposition makeDecomposition() {
		return transformationFactory.makeDecomposition(dispatchor.getDecompositionKey(),
				dispatchor.getDecompositionOptions());
	}

	@Override
	public CoordinatesToValues produceOutputColumn(List<CoordinatesToValues> underlyings) {
		if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		} else if (underlyings.size() != 1) {
			throw new IllegalArgumentException("A dispatchor expects a single underlying");
		}

		MultiTypeStorage<Map<String, ?>> aggregatingView = MultiTypeStorage.<Map<String, ?>>builder().build();

		IAggregation agg = transformationFactory.makeAggregation(dispatchor.getAggregationKey());

		IDecomposition decomposition = makeDecomposition();

		for (Map<String, ?> coordinate : BucketorQueryStep.keySet(dispatchor.isDebug(), underlyings)) {
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
				Map<Map<String, ?>, Object> decomposed = decomposition.decompose(coordinate, value);

				decomposed.forEach((fragmentCoordinate, fragmentValue) -> {
					if (dispatchor.isDebug()) {
						log.info("Contribute {} into {}", fragmentValue, fragmentCoordinate);
					}

					aggregatingView.merge(fragmentCoordinate, fragmentValue, agg);
				});
			}
		}

		return CoordinatesToValues.builder().storage(aggregatingView).build();
	}
}
