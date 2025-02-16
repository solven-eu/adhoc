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
package eu.solven.adhoc.measure.step;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.IMeasure;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.combination.FindFirstCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
import eu.solven.adhoc.storage.ISliceAndValueConsumer;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.SliceToValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IHasUnderlyingQuerySteps} for {@link Filtrator}
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class FiltratorQueryStep extends AHasUnderlyingQuerySteps {
	final Filtrator filtrator;
	final IOperatorsFactory transformationFactory;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return filtrator.getUnderlyingNames();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		AdhocQueryStep underlyingStep = AdhocQueryStep.edit(step)
				.filter(AndFilter.and(step.getFilter(), filtrator.getFilter()))
				.measure(ReferencedMeasure.builder().ref(filtrator.getUnderlying()).build())
				.build();
		return Collections.singletonList(underlyingStep);
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		} else if (underlyings.size() != 1) {
			throw new IllegalArgumentException("underlyings.size() != 1");
		}

		ISliceToValue output = makeCoordinateToValues();

		forEachDistinctSlice(underlyings, new FindFirstCombination(), output::putSlice);

		return output;
	}

	protected ISliceToValue makeCoordinateToValues() {
		return SliceToValue.builder().build();
	}

	@Override
	protected IMeasure getMeasure() {
		return filtrator;
	}

	@Override
	protected AdhocQueryStep getStep() {
		return step;
	}

	@Override
	protected void onSlice(List<? extends ISliceToValue> underlyings,
			IAdhocSliceWithStep slice,
			ICombination combination,
			ISliceAndValueConsumer output) {
		List<Object> underlyingVs = underlyings.stream().map(storage -> {
			AtomicReference<Object> refV = new AtomicReference<>();

			storage.onValue(slice, refV::set);

			return refV.get();
		}).collect(Collectors.toList());

		Object value = combination.combine(slice, underlyingVs);

		if (isDebug()) {
			log.info("[DEBUG] Write {} (given {}) in {} for {}", value, underlyingVs, slice, getMeasure().getName());
		}

		output.putSlice(slice.getAdhocSliceAsMap(), value);
	}
}
