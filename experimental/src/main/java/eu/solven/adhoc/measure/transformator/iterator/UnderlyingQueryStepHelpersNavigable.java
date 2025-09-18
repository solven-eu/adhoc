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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.column.navigable.MultitypeNavigableColumn;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.primitive.IValueProvider;
import lombok.extern.slf4j.Slf4j;

/**
 * Enable merging {@link Stream} of {@link SliceAndMeasures}, expected most of them to be
 * {@link MultitypeNavigableColumn}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class UnderlyingQueryStepHelpersNavigable {
	protected UnderlyingQueryStepHelpersNavigable() {
		// hidden
	}

	/**
	 * @param queryStep
	 *            the considered queryStep.
	 * @param underlyings
	 *            the underlyings for given queryStep.
	 * @return the union-Set of slices
	 */
	public static Stream<SliceAndMeasures> distinctSlices(CubeQueryStep queryStep,
			List<? extends ISliceToValue> underlyings) {
		boolean debug = queryStep.isDebug();

		if (!debug && underlyings.size() == 1) {
			// Fast track
			ISliceToValue underlying = underlyings.getFirst();
			return underlying.stream().map(slice -> {
				Object value = IValueProvider.getValue(slice.getValueProvider());

				return SliceAndMeasures.from(slice.getSlice(), queryStep, Collections.singletonList(value));
			});
		}

		// It is unclear how to implement an algorithm handling both sorted and hash columns
		// We would have to de-duplicates slices, and merge measures. To be tested and benchmarked.
		if (underlyings.stream().anyMatch(sv -> !sv.isEmpty() && !sv.isSorted())) {
			// One of the underlying is not sorted: this will output a not-sorted merge distinct
			// TODO It is a pity to lose all sorted available information due to one (potentially very small) not
			// sorted column.

			// Merge all SliceAsMap in a Set
			Set<IAdhocSlice> notSortedAsSet;
			if (underlyings.isEmpty()) {
				notSortedAsSet = Set.of();
			} else if (underlyings.size() == 1) {
				notSortedAsSet = underlyings.iterator().next().slicesSet();
			} else {
				notSortedAsSet = underlyings.stream().flatMap(ISliceToValue::slices).collect(Collectors.toSet());
			}

			int size = underlyings.size();
			return notSortedAsSet.stream().map(sliceAsMap -> {
				List<Object> underlyingVs = new ArrayList<>(size);
				for (int i = 0; i < size; i++) {
					underlyingVs.add(ISliceToValue.getValue(underlyings.get(i), sliceAsMap));
				}

				return SliceAndMeasures.from(sliceAsMap, queryStep, underlyingVs);
			});

		} else {
			// All underlyings are sorted (typically from MultitypeColumnNavigable)
			List<ISliceToValue> sorted = new ArrayList<>();

			for (ISliceToValue slices : underlyings) {
				if (slices.isEmpty()) {
					sorted.add(SliceToValue.empty());
				} else {
					sorted.add(slices);
				}
			}

			return mergeSortedStreamDistinct(queryStep, underlyings);
		}
	}

	/**
	 * 
	 * @param queryStep
	 * @param sorted
	 *            {@link ISliceToValue} required to be sorted.
	 * @return a merged {@link Stream}, based on the input sorted ISliceToValue
	 */
	private static Stream<SliceAndMeasures> mergeSortedStreamDistinct(CubeQueryStep queryStep,
			List<? extends ISliceToValue> sorted) {
		List<Iterator<SliceAndMeasure<IAdhocSlice>>> sortedIterators = sorted.stream().peek(s -> {
			if (!s.isSorted()) {
				throw new IllegalArgumentException(
						"This requires input Stream to be sorted. queryStep=%s".formatted(queryStep));
			}
		}).map(s -> s.stream().iterator()).toList();

		Iterator<SliceAndMeasures> mergedIterator = new MergedSlicesIteratorNavigable(queryStep, sortedIterators);

		int characteristics = 0
				// keys are sorted naturally
				| Spliterator.ORDERED
				| Spliterator.SORTED
				// When read, this can not be edited anymore
				| Spliterator.IMMUTABLE;
		Spliterator<SliceAndMeasures> spliterator =
				Spliterators.spliteratorUnknownSize(mergedIterator, characteristics);
		return StreamSupport.stream(spliterator, false);
	}

}
