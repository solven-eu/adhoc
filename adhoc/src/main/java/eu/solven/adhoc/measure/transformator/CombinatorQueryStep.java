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
package eu.solven.adhoc.measure.transformator;

import java.util.List;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.query.StandardQueryOptions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CombinatorQueryStep extends ATransformator {
	final ICombinator combinator;
	final IOperatorsFactory operatorsFactory;
	@Getter
	final CubeQueryStep step;

	final Supplier<ICombination> combinationSupplier = Suppliers.memoize(this::makeCombination);

	protected ICombination makeCombination() {
		return operatorsFactory.makeCombination(combinator);
	}

	public List<String> getUnderlyingNames() {
		return combinator.getUnderlyingNames();
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		return getUnderlyingNames().stream()
				// Change the requested measureName to the underlying measureName
				.map(underlyingName -> CubeQueryStep.edit(step).measure(underlyingName).build())
				.toList();
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() != getUnderlyingNames().size()) {
			throw new IllegalArgumentException("underlyingNames.size() != underlyings.size() (%s, %s)"
					.formatted(getUnderlyingNames(), underlyings.size()));
		} else if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		}

		IMultitypeColumnFastGet<SliceAsMap> storage = makeStorage();

		ICombination combination = combinationSupplier.get();

		forEachDistinctSlice(underlyings, combination, storage::append);

		return SliceToValue.builder().column(storage).build();
	}

	@Override
	protected void onSlice(List<? extends ISliceToValue> underlyings,
			SliceAndMeasures slice,
			ICombination combination,
			ISliceAndValueConsumer output) {

		ISlicedRecord slicedRecord = slice.getMeasures();
		try {
			IValueProvider valueProvider = combine(slice.getSlice(), combination, slicedRecord);

			valueProvider.acceptConsumer(output.putSlice(slice.getSlice().getAdhocSliceAsMap()));
		} catch (RuntimeException e) {
			if (step.getOptions().contains(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)) {
				output.putSlice(slice.getSlice().getAdhocSliceAsMap()).onObject(e);
			} else {
				throw new IllegalArgumentException(
						"Issue evaluating %s over %s in %s".formatted(combinator.getName(), slicedRecord, slice),
						e);
			}
		}
	}

	protected IValueProvider combine(ISliceWithStep slice, ICombination combination, ISlicedRecord slicedRecord) {
		IValueProvider valueProvider = combination.combine(slice, slicedRecord);

		if (isDebug()) {
			log.info("[DEBUG] Write {} (given {} over {}) in {}",
					IValueProvider.getValue(valueProvider),
					combinator.getName(),
					slicedRecord,
					slice);
		}
		return valueProvider;
	}

}
