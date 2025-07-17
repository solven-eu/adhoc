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
package eu.solven.adhoc.measure.transformator.step;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.transformator.ATransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.ICombinator;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingNames;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.query.StandardQueryOptions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ITransformatorQueryStep} for {@link Combinator}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class CombinatorQueryStep extends ATransformatorQueryStep {
	final ICombinator combinator;
	@Getter(AccessLevel.PROTECTED)
	final AdhocFactories factories;

	@Getter
	final CubeQueryStep step;

	final Supplier<ICombination> combinationSupplier = Suppliers.memoize(this::makeCombination);

	protected ICombination makeCombination() {
		return factories.getOperatorFactory().makeCombination(combinator);
	}

	public List<String> getUnderlyingNames() {
		ICombination combination = combinationSupplier.get();
		if (combination instanceof IHasUnderlyingNames hasUnderlyingNames) {
			// Happens on some ICombination, like those parsing an expression
			return hasUnderlyingNames.getUnderlyingNames();
		} else {
			return combinator.getUnderlyingNames();
		}
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		List<String> names = getUnderlyingNames();

		if (names.isEmpty()) {
			// This measure has no explicit underlyings: We add an implicit EmptyAggregator: it will materialize
			// the slices with no aggregate
			return List.of(CubeQueryStep.edit(step).measure(Aggregator.empty()).build());
		}

		return names.stream()
				// Change the requested measureName to the underlying measureName
				.map(underlyingName -> CubeQueryStep.edit(step).measure(underlyingName).build())
				.toList();
	}

	public static int sumSizes(Collection<? extends ISliceToValue> underlyings) {
		return Ints.saturatedCast(underlyings.stream().mapToLong(ISliceToValue::size).sum());
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (getUnderlyingNames().isEmpty() && underlyings.size() == 1) {
			// The provided column is probably computed for `EmptyAggregation`
			log.trace("Received EmptyAggregation sliceToValue");
		} else if (underlyings.size() != getUnderlyingNames().size()) {
			throw new IllegalArgumentException("underlyingNames.size() != underlyings.size() (%s, %s)"
					.formatted(getUnderlyingNames(), underlyings.size()));
		} else if (underlyings.isEmpty()) {
			// BEWARE This prevents a Combinator to return slices independently of underlyings
			return SliceToValue.empty();
		}

		ICombination combination = combinationSupplier.get();
		if (CoalesceCombination.isFindFirst(combination) && underlyings.size() == 1) {
			// Shortcut given Coalesce specific semantic
			return underlyings.getFirst();
		}

		IMultitypeColumnFastGet<IAdhocSlice> values = factories.getColumnFactory().makeColumn(sumSizes(underlyings));

		forEachDistinctSlice(underlyings, combination, values::append);

		return SliceToValue.forGroupBy(step).values(values).build();
	}

	@Override
	protected void onSlice(SliceAndMeasures slice, ICombination combination, ISliceAndValueConsumer output) {
		ISlicedRecord slicedRecord = slice.getMeasures();
		IValueReceiver outputSlice = output.putSlice(slice.getSlice().getSlice());
		try {
			IValueProvider valueProvider = combine(slice.getSlice(), combination, slicedRecord);

			valueProvider.acceptReceiver(outputSlice);
		} catch (RuntimeException e) {
			if (step.getOptions().contains(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)) {
				outputSlice.onObject(e);
			} else {
				throw new IllegalArgumentException(
						"Issue evaluating %s over %s in %s".formatted(combinator.getName(), slicedRecord, slice),
						e);
			}
		}
	}

	protected IValueProvider combine(ISliceWithStep slice,
			// ICombinationBinding binded,
			ICombination combination,
			ISlicedRecord slicedRecord) {
		IValueProvider valueProvider = combination.combine(slice, slicedRecord);

		if (isDebug()) {
			log.info("[DEBUG] Write {}={} ({} over {}) in {}",
					combinator.getName(),
					IValueProvider.getValue(valueProvider),
					combinator.getCombinationKey(),
					slicedRecord,
					slice);
		}
		return valueProvider;
	}

}
