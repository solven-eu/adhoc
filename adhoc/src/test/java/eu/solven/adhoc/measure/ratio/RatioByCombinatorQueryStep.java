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
package eu.solven.adhoc.measure.ratio;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.dag.ICoordinatesAndValueConsumer;
import eu.solven.adhoc.dag.ICoordinatesToValues;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.transformers.AHasUnderlyingQuerySteps;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class RatioByCombinatorQueryStep extends AHasUnderlyingQuerySteps {
	final RatioByCombinator combinator;
	final IOperatorsFactory transformationFactory;
	@Getter
	final AdhocQueryStep step;

	@Override
	protected IMeasure getMeasure() {
		return combinator;
	}

	public List<String> getUnderlyingNames() {
		return combinator.getUnderlyingNames();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		String underlying = combinator.getUnderlying();

		AdhocQueryStep numerator = AdhocQueryStep.edit(step)
				// Change the requested measureName to the underlying measureName
				.measure(ReferencedMeasure.builder().ref(underlying).build())
				.filter(AndFilter.and(step.getFilter(), combinator.getNumeratorFilter()))
				.build();

		AdhocQueryStep denominator = AdhocQueryStep.edit(step)
				// Change the requested measureName to the underlying measureName
				.measure(ReferencedMeasure.builder().ref(underlying).build())
				.filter(AndFilter.and(step.getFilter(), combinator.getDenominatorFilter()))
				.build();

		return Arrays.asList(numerator, denominator);
	}

	@Override
	public ICoordinatesToValues produceOutputColumn(List<? extends ICoordinatesToValues> underlyings) {
		if (underlyings.size() != 2) {
			throw new IllegalArgumentException("Expected 2 underlyings. Got %s".formatted(underlyings.size()));
		}

		ICoordinatesToValues output = makeCoordinateToValues();

		ICombination transformation = transformationFactory.makeCombination(combinator);

		forEachDistinctSlice(underlyings, transformation, output);

		return output;
	}

	@Override
	protected void onSlice(List<? extends ICoordinatesToValues> underlyings,
			IAdhocSliceWithStep slice,
			ICombination combination,
			ICoordinatesAndValueConsumer output) {
		List<Object> underlyingVs = underlyings.stream().map(storage -> {
			AtomicReference<Object> refV = new AtomicReference<>();
			AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(refV::set);

			storage.onValue(slice, consumer);

			return refV.get();
		}).collect(Collectors.toList());

		Object value = combination.combine(slice, underlyingVs);

		if (isDebug()) {
			log.info("[DEBUG] Write {} (given {}) in {} for {}", value, underlyingVs, slice, combinator.getName());
		}

		output.put(slice.getAdhocSliceAsMap(), value);
	}

	protected ICoordinatesToValues makeCoordinateToValues() {
		return CoordinatesToValues.builder().build();
	}

}
