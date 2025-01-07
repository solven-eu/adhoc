/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.transformers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.IDecomposition;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.dag.ICoordinatesToValues;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.slice.AdhocSliceAsMapWithCustom;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DispatchorQueryStep implements IHasUnderlyingQuerySteps {
	final Dispatchor dispatchor;
	final IOperatorsFactory transformationFactory;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return dispatchor.getUnderlyingNames();
	}

	;

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		IDecomposition decomposition = makeDecomposition();

		return decomposition.getUnderlyingSteps(step).stream().map(subStep -> {
			return AdhocQueryStep.edit(subStep)
					.measure(ReferencedMeasure.builder().ref(dispatchor.getUnderlying()).build())
					.build();
		}).collect(Collectors.toList());

	}

	private IDecomposition makeDecomposition() {
		return transformationFactory.makeDecomposition(dispatchor.getDecompositionKey(),
				dispatchor.getDecompositionOptions());
	}

	@Override
	public ICoordinatesToValues produceOutputColumn(List<? extends ICoordinatesToValues> underlyings) {
		if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		} else if (underlyings.size() != 1) {
			throw new IllegalArgumentException("A dispatchor expects a single underlying");
		}

		IAggregation agg = transformationFactory.makeAggregation(dispatchor.getAggregationKey());

		MultiTypeStorage<Map<String, ?>> aggregatingView =
				MultiTypeStorage.<Map<String, ?>>builder().aggregation(agg).build();

		IDecomposition decomposition = makeDecomposition();

		for (Map<String, ?> coordinates : ColumnatorQueryStep.keySet(dispatchor.isDebug(), underlyings)) {
			AdhocSliceAsMapWithCustom slice = AdhocSliceAsMapWithCustom.builder()
					.slice(AdhocSliceAsMap.fromMap(coordinates))
					.queryStep(step)
					.build();

			onSlice(underlyings, coordinates, slice, decomposition, aggregatingView);
		}

		return CoordinatesToValues.builder().storage(aggregatingView).build();
	}

	protected void onSlice(List<? extends ICoordinatesToValues> underlyings,
			Map<String, ?> coordinate,
			AdhocSliceAsMapWithCustom slice,
			IDecomposition decomposition,
			MultiTypeStorage<Map<String, ?>> aggregatingView) {
		List<Object> underlyingVs = underlyings.stream().map(storage -> {
			AtomicReference<Object> refV = new AtomicReference<>();
			AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(refV::set);

			storage.onValue(slice, consumer);

			return refV.get();
		}).toList();

		Object value = underlyingVs.getFirst();

		if (value != null) {
			Map<Map<String, ?>, Object> decomposed = decomposition.decompose(coordinate, value);

			decomposed.forEach((fragmentCoordinate, fragmentValue) -> {
				if (dispatchor.isDebug()) {
					log.info("Contribute {} into {}", fragmentValue, fragmentCoordinate);
				}

				aggregatingView.merge(fragmentCoordinate, fragmentValue);
			});
		}
	}
}
