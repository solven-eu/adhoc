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

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.transformator.ATransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.query.filter.AndFilter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class RatioByCombinatorQueryStep extends ATransformatorQueryStep {
	final RatioByCombinator combinator;
	@Getter(AccessLevel.PROTECTED)
	final AdhocFactories factories;

	@Getter
	final CubeQueryStep step;

	public List<String> getUnderlyingNames() {
		return combinator.getUnderlyingNames();
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		String underlying = combinator.getUnderlying();

		CubeQueryStep numerator = CubeQueryStep.edit(step)
				// Change the requested measureName to the underlying measureName
				.measure(underlying)
				.filter(AndFilter.and(step.getFilter(), combinator.getNumeratorFilter()))
				.build();

		CubeQueryStep denominator = CubeQueryStep.edit(step)
				// Change the requested measureName to the underlying measureName
				.measure(underlying)
				.filter(AndFilter.and(step.getFilter(), combinator.getDenominatorFilter()))
				.build();

		return Arrays.asList(numerator, denominator);
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() != 2) {
			throw new IllegalArgumentException("Expected 2 underlyings. Got %s".formatted(underlyings.size()));
		}

		IMultitypeColumnFastGet<IAdhocSlice> values = makeStorage();

		ICombination transformation = factories.getOperatorFactory().makeCombination(combinator);

		forEachDistinctSlice(underlyings, transformation, values::append);

		return SliceToValue.forGroupBy(step).values(values).build();
	}

	@Override
	protected void onSlice(SliceAndMeasures slice, ICombination combination, ISliceAndValueConsumer output) {
		List<?> underlyingVs = slice.getMeasures().asList();

		Object value = combination.combine(slice.getSlice(), underlyingVs);

		if (isDebug()) {
			log.info("[DEBUG] Write {} (given {}) in {} for {}", value, underlyingVs, slice, combinator.getName());
		}

		output.putSlice(slice.getSlice().getSlice()).onObject(value);
	}

	protected IMultitypeColumnFastGet<IAdhocSlice> makeStorage() {
		return MultitypeHashColumn.<IAdhocSlice>builder().build();
	}

}
