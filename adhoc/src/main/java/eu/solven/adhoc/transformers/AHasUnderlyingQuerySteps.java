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

import java.util.List;
import java.util.function.Consumer;

import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.ICoordinatesAndValueConsumer;
import eu.solven.adhoc.dag.ICoordinatesToValues;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.slice.AdhocSliceAsMapWithStep;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds default behaviors used by most {@link IHasUnderlyingQuerySteps}
 */
@RequiredArgsConstructor
@Slf4j
public abstract class AHasUnderlyingQuerySteps implements IHasUnderlyingQuerySteps {

	protected abstract IMeasure getMeasure();

	// public List<String> getUnderlyingNames() {
	// return getMeasure().getUnderlyingNames();
	// }

	protected abstract AdhocQueryStep getStep();

	protected boolean isDebug() {
		return getMeasure().isDebug() || getStep().isDebug();
	}

	protected void forEachDistinctSlice(List<? extends ICoordinatesToValues> underlyings,
			ICombination combination,
			ICoordinatesAndValueConsumer output) {
		forEachDistinctSlice(underlyings, slice -> onSlice(underlyings, slice, combination, output));
	}

	protected void forEachDistinctSlice(List<? extends ICoordinatesToValues> underlyings,
			Consumer<AdhocSliceAsMapWithStep> sliceConsumer) {
		Iterable<? extends AdhocSliceAsMap> distinctSlices = distinctSlices(underlyings);

		int slicesDone = 0;
		for (AdhocSliceAsMap coordinates : distinctSlices) {
			AdhocSliceAsMapWithStep slice =
					AdhocSliceAsMapWithStep.builder().slice(coordinates).queryStep(getStep()).build();

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

	private Iterable<? extends AdhocSliceAsMap> distinctSlices(List<? extends ICoordinatesToValues> underlyings) {
		return UnderlyingQueryStepHelpers.distinctSlices(isDebug(), underlyings);
	}

	protected abstract void onSlice(List<? extends ICoordinatesToValues> underlyings,
			IAdhocSliceWithStep slice,
			ICombination combination,
			ICoordinatesAndValueConsumer output);
}
