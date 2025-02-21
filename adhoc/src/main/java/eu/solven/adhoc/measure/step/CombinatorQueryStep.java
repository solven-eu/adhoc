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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.IMeasure;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
import eu.solven.adhoc.storage.ISliceAndValueConsumer;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.SliceToValue;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CombinatorQueryStep extends ATransformator {
	final ICombinator combinator;
	final IOperatorsFactory operatorsFactory;
	@Getter
	final AdhocQueryStep step;

	final Supplier<ICombination> combinationSupplier = Suppliers.memoize(this::makeCombination);

	protected ICombination makeCombination() {
		return operatorsFactory.makeCombination(combinator);
	}

	@Override
	protected IMeasure getMeasure() {
		return combinator;
	}

	public List<String> getUnderlyingNames() {
		return combinator.getUnderlyingNames();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		return getUnderlyingNames().stream().map(underlyingName -> {
			return AdhocQueryStep.edit(step)
					// Change the requested measureName to the underlying measureName
					.measureNamed(underlyingName)
					.build();
		}).toList();
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() != getUnderlyingNames().size()) {
			throw new IllegalArgumentException("underlyingNames.size() != underlyings.size()");
		} else if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		}

		ISliceToValue output = makeCoordinateToValues();

		ICombination transformation = combinationSupplier.get();

		forEachDistinctSlice(underlyings, transformation, output);

		return output;
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

		Object value = combine(slice, combination, underlyingVs);

		output.putSlice(slice.getAdhocSliceAsMap(), value);
	}

	protected Object combine(IAdhocSliceWithStep slice, ICombination combination, List<Object> underlyingVs) {
		Object value;
		try {
			value = combination.combine(slice, underlyingVs);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(
					"Issue evaluating %s over %s in %s".formatted(combinator.getName(), underlyingVs, slice));
		}

		if (isDebug()) {
			log.info("[DEBUG] Write {} (given {} over {}) in {}", value, combinator.getName(), underlyingVs, slice);
		}
		return value;
	}

	protected ISliceToValue makeCoordinateToValues() {
		return SliceToValue.builder().build();
	}

}
