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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.dag.ICoordinatesToValues;
import eu.solven.adhoc.execute.GroupByHelpers;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.slice.AdhocSliceAsMapWithStep;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
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
		return bucketor.getUnderlyings();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		return getUnderlyingNames().stream().map(underlying -> {
			IAdhocGroupBy groupBy = GroupByHelpers.union(step.getGroupBy(), bucketor.getGroupBy());
			AdhocQueryStep object = AdhocQueryStep.edit(step)
					.groupBy(groupBy)
					.measure(ReferencedMeasure.builder().ref(underlying).build())
					.build();
			return object;
		}).collect(Collectors.toList());
	}

	protected boolean isDebug() {
		return bucketor.isDebug() || step.isDebug();
	}

	@Override
	public ICoordinatesToValues produceOutputColumn(List<? extends ICoordinatesToValues> underlyings) {
		if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		IAggregation agg = transformationFactory.makeAggregation(bucketor.getAggregationKey());

		MultiTypeStorage<AdhocSliceAsMap> aggregatingView =
				MultiTypeStorage.<AdhocSliceAsMap>builder().aggregation(agg).build();

		ICombination combinator =
				transformationFactory.makeCombination(bucketor.getCombinationKey(), getCombinationOptions());

		List<String> underlyingNames = getUnderlyingNames();

		for (AdhocSliceAsMap rawSlice : UnderlyingQueryStepHelpers.distinctSlices(isDebug(), underlyings)) {
			AdhocSliceAsMapWithStep slice = AdhocSliceAsMapWithStep.builder().slice(rawSlice).queryStep(step).build();
			onSlice(underlyings, slice, combinator, underlyingNames, aggregatingView);
		}

		return CoordinatesToValues.builder().storage(aggregatingView).build();
	}

	protected void onSlice(List<? extends ICoordinatesToValues> underlyings,
			IAdhocSliceWithStep slice,
			ICombination combinator,
			List<String> underlyingNames,
			MultiTypeStorage<AdhocSliceAsMap> aggregatingView) {
		List<Object> underlyingVs = underlyings.stream().map(storage -> {
			AtomicReference<Object> refV = new AtomicReference<>();
			AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(refV::set);

			storage.onValue(slice.getAdhocSliceAsMap(), consumer);

			return refV.get();
		}).collect(Collectors.toList());

		Object value = combinator.combine(slice, underlyingVs);

		if (isDebug()) {
			Map<String, Object> underylingVsAsMap = new TreeMap<>();

			for (int i = 0; i < underlyingNames.size(); i++) {
				underylingVsAsMap.put(underlyingNames.get(i), underlyingVs.get(i));
			}

			log.info("[DEBUG] m={} c={} transformed {} into {} at {}",
					bucketor.getName(),
					bucketor.getCombinationKey(),
					underylingVsAsMap,
					value,
					slice);
		}

		if (value != null) {
			Map<String, ?> outputCoordinate = queryGroupBy(step.getGroupBy(), slice);

			if (isDebug()) {
				log.info("[DEBUG] m={} contributed {} into {}", bucketor.getName(), value, outputCoordinate);
			}

			aggregatingView.merge(AdhocSliceAsMap.fromMap(outputCoordinate), value);
		}
	}

	protected Map<String, ?> getCombinationOptions() {
		return Combinator.makeAllOptions(bucketor, bucketor.getCombinationOptions());
	}

	protected Map<String, ?> queryGroupBy(IAdhocGroupBy queryGroupBy, IAdhocSliceWithStep slice) {
		Map<String, Object> queryCoordinates = new HashMap<>();

		queryGroupBy.getGroupedByColumns().forEach(groupBy -> {
			Object value = slice.getRawFilter(groupBy);

			if (value == null) {
				// Should we accept null a coordinate, e.g. to handle input partial Maps?
				throw new IllegalStateException("A coordinate-value can not be null");
			}

			queryCoordinates.put(groupBy, value);
		});

		return queryCoordinates;
	}
}
