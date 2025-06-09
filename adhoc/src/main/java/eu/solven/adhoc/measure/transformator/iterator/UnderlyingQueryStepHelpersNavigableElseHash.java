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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.primitives.Booleans;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeNavigableElseHashColumn;
import eu.solven.adhoc.data.column.StreamStrategy;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper around {@link ISliceToValue}.
 * 
 * @author Benoit Lacelle
 * @see MultitypeNavigableElseHashColumn
 */
@Slf4j
public class UnderlyingQueryStepHelpersNavigableElseHash {
	protected UnderlyingQueryStepHelpersNavigableElseHash() {
		// hidden
	}

	/**
	 * @param queryStep
	 *            the considered queryStep.
	 * @param underlyings
	 *            the underlyings for given queryStep.
	 * @return the union-Set of slices
	 */
	@SuppressWarnings("PMD.LinguisticNaming")
	public static Stream<SliceAndMeasures> distinctSlices(CubeQueryStep queryStep,
			List<? extends ISliceToValue> underlyings) {
		boolean debug = queryStep.isDebug();

		if (underlyings.isEmpty()) {
			return Stream.empty();
		} else if (!debug && underlyings.size() == 1) {
			// Fast track
			ISliceToValue underlying = underlyings.getFirst();
			return underlying.stream().map(slice -> {
				Object value = IValueProvider.getValue(slice.getValueProvider());

				return SliceAndMeasures.from(slice.getSlice(), queryStep, Collections.singletonList(value));
			});
		}

		Stream<SliceAndMeasures> sortedSlices = mergeSortedStreamDistinct(queryStep, underlyings);

		boolean[] hasNotSorted = new boolean[underlyings.size()];

		for (int i = 0; i < underlyings.size(); i++) {
			// TODO We may have count the number of notSorted processed slices, to know if there is no more pending
			// slices
			if (underlyings.get(i).stream(StreamStrategy.SORTED_SUB_COMPLEMENT).findAny().isPresent()) {
				hasNotSorted[i] = true;
			}
		}

		if (!Booleans.contains(hasNotSorted, true)) {
			// Not a single notSorted leg
			return sortedSlices;
		}

		// This is expensive: collected all sorted slices, to skip them from notSorted legs
		Set<SliceAsMap> sortedSlicesAsSet = new HashSet<>();

		// BEWARE: This will fill `sortedSlicesAsSet` lazily, while iterating along sortedSlices, before processing
		// notSorted slices. This is broken if one`.parallel` the stream.
		sortedSlices = sortedSlices.peek(s -> {
			sortedSlicesAsSet.add(s.getSlice().getAdhocSliceAsMap());
		});

		List<Stream<SliceAndMeasures>> unsortedStreams = unsortedStreams(queryStep, underlyings, sortedSlicesAsSet);

		// Process ordered slices in priority. Good for MultitypeNavigableElseHashColumn
		return Stream.concat(sortedSlices, unsortedStreams.stream().flatMap(s -> s));
	}

	/**
	 * 
	 * @param queryStep
	 * @param underlyings
	 *            {@link ISliceToValue} required to be sorted.
	 * @return a merged {@link Stream}, based on the input sorted ISliceToValue
	 */
	private static Stream<SliceAndMeasures> mergeSortedStreamDistinct(CubeQueryStep queryStep,
			List<? extends ISliceToValue> underlyings) {
		// boolean[] isNotSorted = new boolean[underlyings.size()];
		//
		// for (int i = 0; i < underlyings.size(); i++) {
		// if (!underlyings.get(i).isSorted()) {
		// isNotSorted[i] = true;
		// }
		// }

		// Exclude the notSortedIterators: they will be processed in a later step
		List<Iterator<SliceAndMeasure<SliceAsMap>>> sortedIterators = underlyings.stream().map(s -> {
			// if (s.isSorted()) {
			// return s.stream().iterator();
			// } else {
			// return Collections.<SliceAndMeasure<SliceAsMap>>emptyIterator();
			// }
			return s.stream(StreamStrategy.SORTED_SUB).iterator();
		}).toList();

		Iterator<SliceAndMeasures> mergedIterator = new MergedSlicesIteratorNavigableElseHash(queryStep, sortedIterators
		// , isNotSorted
				, underlyings);

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

	/**
	 * 
	 * @param queryStep
	 * @param underlyings
	 * @param sortedSlicesAsSet
	 *            the Set of slices which were already processed by the sorted streams. This may be filled lazily, which
	 *            is fine as long as unsorted streams are consumed strictly after the sorted streams are consumed.
	 * @return a List of {@link Stream} handling unsorted columns
	 */
	private static List<Stream<SliceAndMeasures>> unsortedStreams(CubeQueryStep queryStep,
			List<? extends ISliceToValue> underlyings,
			Set<SliceAsMap> sortedSlicesAsSet) {
		Set<SliceAsMap> unsortedSlicesAsSet = new HashSet<>();

		List<Stream<SliceAndMeasures>> unsortedStreams = new ArrayList<>();

		int size = underlyings.size();
		for (int rawI = 0; rawI < size; rawI++) {
			int i = rawI;
			ISliceToValue sliceToValue = underlyings.get(i);
			Stream<SliceAndMeasures> unsortedStream = sliceToValue.stream(StreamStrategy.SORTED_SUB_COMPLEMENT)
					// Skip the slices already processed by sorted iterators
					.filter(s -> !sortedSlicesAsSet.contains(s.getSlice()))
					// Skip the slices processed by unsorted iterators
					.filter(s -> !unsortedSlicesAsSet.contains(s.getSlice()))
					.map(s -> {
						SliceAndMeasure<SliceAsMap> sliceAndMeasure = s;

						List<IValueProvider> valueProviders = new ArrayList<>(size);

						// No point is processing previous columns are their slices are already processed
						for (int ii = 0; ii < i; ii++) {
							valueProviders.add(IValueProvider.NULL);
						}
						for (int ii = i; ii < size; ii++) {
							if (ii == i) {
								// Current column
								valueProviders.add(s.getValueProvider());
							} else {
								ISliceToValue underlying = underlyings.get(ii);
								if (!underlying.isSorted()) {
									// Only unsorted columns are candidate
									valueProviders.add(underlying.onValue(s.getSlice()));
								} else {
									// sorted columns got all they slices processed previously
									valueProviders.add(IValueProvider.NULL);
								}
							}
						}

						// Prevent this slice to be considered by next unsorted sliceToValues
						unsortedSlicesAsSet.add(s.getSlice());

						return SliceAndMeasures.from(queryStep, sliceAndMeasure.getSlice(), valueProviders);
					});

			unsortedStreams.add(unsortedStream);
		}
		return unsortedStreams;
	}
}
