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
import java.util.function.Consumer;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.slice.ISliceWithStep;
import eu.solven.adhoc.slice.SliceAsMap;
import eu.solven.adhoc.slice.SliceAsMapWithStep;
import eu.solven.adhoc.storage.IMultitypeColumnFastGet;
import eu.solven.adhoc.storage.ISliceAndValueConsumer;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.MultiTypeStorageFastGet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds default behaviors used by most {@link ITransformator}
 */
@RequiredArgsConstructor
@Slf4j
public abstract class ATransformator implements ITransformator {

	protected abstract IMeasure getMeasure();

	protected abstract AdhocQueryStep getStep();

	protected boolean isDebug() {
		return getMeasure().isDebug() || getStep().isDebug();
	}

	protected IMultitypeColumnFastGet<SliceAsMap> makeStorage() {
		return MultiTypeStorageFastGet.<SliceAsMap>builder().build();
	}

	protected void forEachDistinctSlice(List<? extends ISliceToValue> underlyings,
			ICombination combination,
			ISliceAndValueConsumer output) {
		forEachDistinctSlice(underlyings, slice -> onSlice(underlyings, slice, combination, output));
	}

	protected void forEachDistinctSlice(List<? extends ISliceToValue> underlyings,
			Consumer<SliceAsMapWithStep> sliceConsumer) {
		Iterable<? extends SliceAsMap> distinctSlices = distinctSlices(underlyings);

		int slicesDone = 0;
		for (SliceAsMap coordinates : distinctSlices) {
			SliceAsMapWithStep slice = SliceAsMapWithStep.builder().slice(coordinates).queryStep(getStep()).build();

			try {
				sliceConsumer.accept(slice);
			} catch (RuntimeException e) {
				throw new IllegalArgumentException(
						"Issue processing m=%s slice=%s".formatted(getMeasure().getName(), slice),
						e);
			}

			if (Integer.bitCount(++slicesDone) == 1) {
				if (isDebug()) {
					log.info("[DEBUG] Done processing {} slices", slicesDone);
				}
			}
		}
	}

	protected Iterable<? extends SliceAsMap> distinctSlices(List<? extends ISliceToValue> underlyings) {
		return UnderlyingQueryStepHelpers.distinctSlices(isDebug(), underlyings);
	}

	protected abstract void onSlice(List<? extends ISliceToValue> underlyings,
			ISliceWithStep slice,
			ICombination combination,
			ISliceAndValueConsumer output);
}
