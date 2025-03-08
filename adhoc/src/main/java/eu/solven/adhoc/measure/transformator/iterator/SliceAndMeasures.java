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
import java.util.function.Consumer;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.record.ISlicedRecord;
import eu.solven.adhoc.slice.ISliceWithStep;
import eu.solven.adhoc.slice.SliceAsMap;
import eu.solven.adhoc.slice.SliceAsMapWithStep;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.IValueConsumer;
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
	 * @param sliceAndMeasures
	 *            may have null if the underlying queryStep did not hold current slice
	 * @return
	 */
	public static SliceAndMeasures from(AdhocQueryStep queryStep,
			SliceAsMap slice,
			List<Consumer<IValueConsumer>> valueConsumerConsumers) {
		return SliceAndMeasures.builder()
				.slice(SliceAsMapWithStep.builder().slice(slice).queryStep(queryStep).build())
				.measures(SlicedRecordFromSlices.builder().valueConsumers(valueConsumerConsumers).build())
				.build();
	}

	public static SliceAndMeasures from(SliceAsMap slice, AdhocQueryStep queryStep, List<?> underlyingVs) {
		return SliceAndMeasures.builder()
				.slice(SliceAsMapWithStep.builder().slice(slice).queryStep(queryStep).build())
				.measures(SlicedRecordFromArray.builder().measures(underlyingVs).build())
				.build();
	}

}
