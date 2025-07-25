/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.transformator.iterator;

import java.util.List;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.step.SliceAsMapWithStep;
import eu.solven.adhoc.primitive.IValueProvider;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Combines a slice and the underlying measures. Typically useful when iterating along the underlying
 * {@link ISliceToValue}.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
public class SliceAndMeasures {
	@NonNull
	ISliceWithStep slice;
	@NonNull
	ISlicedRecord measures;

	/**
	 *
	 * @param queryStep
	 * @param slice
	 * @param valueProviders
	 *            underlyingStep index to a value provider
	 * @return
	 */
	public static SliceAndMeasures from(CubeQueryStep queryStep,
			IAdhocSlice slice,
			List<IValueProvider> valueProviders) {
		return SliceAndMeasures.builder()
				.slice(SliceAsMapWithStep.builder().slice(slice).queryStep(queryStep).build())
				.measures(SlicedRecordFromSlices.builder().valueProviders(valueProviders).build())
				.build();
	}

	public static SliceAndMeasures from(IAdhocSlice slice, CubeQueryStep queryStep, List<?> underlyingVs) {
		return SliceAndMeasures.builder()
				.slice(SliceAsMapWithStep.builder().slice(slice).queryStep(queryStep).build())
				.measures(SlicedRecordFromArray.builder().measures(underlyingVs).build())
				.build();
	}

}
