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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IDecomposition;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.dag.ICoordinatesAndValueConsumer;
import eu.solven.adhoc.dag.ICoordinatesToValues;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.slice.AdhocSliceAsMapWithStep;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DispatchorQueryStep extends AHasUnderlyingQuerySteps implements IHasUnderlyingQuerySteps {
	final Dispatchor dispatchor;
	final IOperatorsFactory transformationFactory;
	@Getter
	final AdhocQueryStep step;

	@Override
	protected IMeasure getMeasure() {
		return dispatchor;
	}

	public List<String> getUnderlyingNames() {
		return dispatchor.getUnderlyingNames();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		IDecomposition decomposition = makeDecomposition();

		List<IWhereGroupbyAdhocQuery> measurelessSteps = decomposition.getUnderlyingSteps(step);

		if (isDebug()) {
			log.info("[DEBUG] {} underlyingSteps given step={}", measurelessSteps, step);
		}

		ReferencedMeasure refToUnderlying = ReferencedMeasure.builder().ref(dispatchor.getUnderlying()).build();
		return measurelessSteps.stream()
				.map(subStep -> AdhocQueryStep.edit(subStep).measure(refToUnderlying).build())
				.collect(Collectors.toList());

	}

	protected IDecomposition makeDecomposition() {
		return transformationFactory.makeDecomposition(dispatchor.getDecompositionKey(),
				dispatchor.getDecompositionOptions());
	}

	@Override
	protected void onSlice(List<? extends ICoordinatesToValues> underlyings,
			IAdhocSliceWithStep slice,
			ICombination combination,
			ICoordinatesAndValueConsumer output) {
		throw new UnsupportedOperationException(
				"Unclear how to refactor IDispator in AHasUnderlyingQuerySteps.onSlice");
	}

	@Override
	public ICoordinatesToValues produceOutputColumn(List<? extends ICoordinatesToValues> underlyings) {
		if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		} else if (underlyings.size() != 1) {
			throw new IllegalArgumentException("A dispatchor expects a single underlying");
		}

		IAggregation agg = transformationFactory.makeAggregation(dispatchor.getAggregationKey());

		MultiTypeStorage<AdhocSliceAsMap> aggregatingView =
				MultiTypeStorage.<AdhocSliceAsMap>builder().aggregation(agg).build();

		IDecomposition decomposition = makeDecomposition();

		forEachDistinctSlice(underlyings, slice -> onSlice(underlyings, slice, decomposition, aggregatingView));

		return CoordinatesToValues.builder().storage(aggregatingView).build();
	}

	protected void onSlice(List<? extends ICoordinatesToValues> underlyings,
			AdhocSliceAsMapWithStep slice,
			IDecomposition decomposition,
			MultiTypeStorage<AdhocSliceAsMap> aggregatingView) {
		List<Object> underlyingVs = underlyings.stream().map(storage -> {
			AtomicReference<Object> refV = new AtomicReference<>();
			AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(refV::set);

			storage.onValue(slice, consumer);

			return refV.get();
		}).toList();

		Object value = underlyingVs.getFirst();

		if (value != null) {
			Map<Map<String, ?>, Object> decomposed = decomposition.decompose(slice, value);

			Set<Map<String, ?>> outputCoordinatesAlreadyContributed = new HashSet<>();

			decomposed.forEach((fragmentCoordinate, fragmentValue) -> {
				if (isDebug()) {
					log.info("[DEBUG] Contribute {} into {}", fragmentValue, fragmentCoordinate);
				}

				Map<String, ?> outputCoordinate = queryGroupBy(step.getGroupBy(), slice, fragmentCoordinate);

				if (outputCoordinatesAlreadyContributed.add(outputCoordinate)) {
					AdhocSliceAsMap coordinateAsSlice = AdhocSliceAsMap.fromMap(outputCoordinate);
					aggregatingView.merge(coordinateAsSlice, fragmentValue);

					if (isDebug()) {
						aggregatingView.onValue(coordinateAsSlice, AsObjectValueConsumer.consumer(o -> {
							log.info("[DEBUG] slice={} has been merged into agg={}", fragmentCoordinate, o);
						}));
					}
				} else {
					log.debug("slice={} has already contributed into {}", slice, outputCoordinate);
				}
			});
		}
	}

	protected Map<String, ?> queryGroupBy(@NonNull IAdhocGroupBy queryGroupBy,
			AdhocSliceAsMapWithStep slice,
			Map<String, ?> fragmentCoordinate) {
		Map<String, Object> queryCoordinates = new HashMap<>();

		queryGroupBy.getGroupedByColumns().forEach(groupBy -> {
			// BEWARE it is legal only to get groupColumns from the fragment coordinate
			Object value = fragmentCoordinate.get(groupBy);

			if (value == null) {
				value = slice.getRawFilter(groupBy);
			}

			if (value == null) {
				// Should we accept null a coordinate, e.g. to handle input partial Maps?
				throw new IllegalStateException("A coordinate-value can not be null");
			}

			queryCoordinates.put(groupBy, value);
		});

		return queryCoordinates;
	}
}
