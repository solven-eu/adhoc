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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeNavigableColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.step.UnderlyingQueryStepHelpers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds default behaviors used by most {@link ITransformatorQueryStep}
 */
@RequiredArgsConstructor
@Slf4j
public abstract class ATransformatorQueryStep implements ITransformatorQueryStep {

	protected abstract CubeQueryStep getStep();

	protected IMeasure getMeasure() {
		return getStep().getMeasure();
	}

	protected boolean isDebug() {
		return getStep().isDebug();
	}

	protected IMultitypeColumnFastGet<SliceAsMap> makeStorage() {
		return MultitypeNavigableColumn.<SliceAsMap>builder().build();
	}

	protected void forEachDistinctSlice(List<? extends ISliceToValue> underlyings,
			ICombination combination,
			ISliceAndValueConsumer output) {
		forEachDistinctSlice(underlyings, slice -> onSlice(underlyings, slice, combination, output));
	}

	protected void forEachDistinctSlice(List<? extends ISliceToValue> underlyings,
			Consumer<SliceAndMeasures> sliceConsumer) {
		Stream<SliceAndMeasures> sliceAndMeasures = distinctSlices(underlyings);

		AtomicInteger slicesDone = new AtomicInteger();
		sliceAndMeasures.forEach(slice -> {
			if (isDebug()) {
				log.info("[DEBUG] Processing slice={}", slice);
			}
			try {
				sliceConsumer.accept(slice);
			} catch (RuntimeException e) {
				throw new IllegalArgumentException(
						"Issue processing m=%s slice=%s".formatted(getMeasure().getName(), slice),
						e);
			}

			if (Integer.bitCount(slicesDone.incrementAndGet()) == 1) {
				if (isDebug()) {
					log.info("[DEBUG] Done processing {} slices", slicesDone);
				}
			}
		});
	}

	protected Stream<SliceAndMeasures> distinctSlices(List<? extends ISliceToValue> underlyings) {
		return UnderlyingQueryStepHelpers.distinctSlices(getStep(), underlyings);
	}

	protected abstract void onSlice(List<? extends ISliceToValue> underlyings,
			SliceAndMeasures slice,
			ICombination combination,
			ISliceAndValueConsumer output);
}
