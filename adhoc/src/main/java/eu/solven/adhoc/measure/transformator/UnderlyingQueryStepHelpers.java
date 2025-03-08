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
package eu.solven.adhoc.measure.transformator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.slice.SliceAsMap;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.IValueConsumer;
import eu.solven.adhoc.storage.SliceToValue;
import eu.solven.adhoc.storage.column.MultitypeHashColumn;
import eu.solven.adhoc.storage.column.MultitypeNavigableColumn;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnderlyingQueryStepHelpers {
	protected UnderlyingQueryStepHelpers() {
		// hidden
	}

	/**
	 * @param debug
	 *            if true, the output set is ordered. This can be quite slow on large sets.
	 * @param underlyings
	 * @return the union-Set of slices
	 */
	public static Stream<SliceAndMeasures> distinctSlices(AdhocQueryStep queryStep,
			List<? extends ISliceToValue> underlyings) {
		boolean debug = queryStep.isDebug() || queryStep.getMeasure().isDebug();

		if (!debug && underlyings.size() == 1) {
			// Fast track
			ISliceToValue underlying = underlyings.getFirst();
			return underlying.stream().map(slice -> {
				Object value = IValueConsumer.getValue(slice.getValueConsumerConsumer());

				return SliceAndMeasures.from(slice.getSlice(), queryStep, Collections.singletonList(value));
			});
		}

		// It is unclear how to implement an algorithm handling both sorted and hash columns
		// We would have to de-duplicates slices, and merge measures. To be tested and benchmarked.
		if (underlyings.stream().anyMatch(sv -> !sv.isEmpty() && !sv.isSorted())) {
			// One of the underlying is not sorted

			// Typically from MultitypeColumnHash
			List<ISliceToValue> notSorted = new ArrayList<>();

			for (ISliceToValue sliceToValue : underlyings) {
				if (sliceToValue.isEmpty()) {
					notSorted.add(SliceToValue.builder().column(MultitypeHashColumn.empty()).build());
				} else {
					notSorted.add(sliceToValue);
				}
			}

			Set<SliceAsMap> notSortedAsSet;
			if (notSorted.isEmpty()) {
				notSortedAsSet = Set.of();
			} else if (notSorted.size() == 1) {
				notSortedAsSet = notSorted.iterator().next().slicesSet();
			} else {
				notSortedAsSet = notSorted.stream().flatMap(s -> s.keySetStream()).collect(Collectors.toSet());
			}

			Stream<SliceAndMeasures> notSortedSlices = notSortedAsSet.stream().map(sliceAsMap -> {
				List<?> underlyingVs = underlyings.stream().map(u -> ISliceToValue.getValue(u, sliceAsMap)).toList();

				return SliceAndMeasures.from(sliceAsMap, queryStep, underlyingVs);
			});

			return notSortedSlices;
		} else {
			// All underlyings are sorted
			// Typically from MultitypeColumnNavigable
			List<ISliceToValue> sorted = new ArrayList<>();

			for (ISliceToValue slices : underlyings) {
				if (slices.isEmpty()) {
					sorted.add(SliceToValue.builder().column(MultitypeNavigableColumn.empty()).build());
				} else {
					sorted.add(slices);
				}
			}

			Stream<SliceAndMeasures> sortedSlices = makeSortedStream(queryStep, sorted);

			return sortedSlices;
		}
	}

	// Similar to Guava Iterators.MergingIterator
	private static class MergedSlicesIterator extends UnmodifiableIterator<SliceAndMeasures> {
		final AdhocQueryStep queryStep;

		final List<PeekingIterator<SliceAndMeasure<SliceAsMap>>> sortedIterators;

		public MergedSlicesIterator(AdhocQueryStep queryStep,
				List<? extends Iterator<SliceAndMeasure<SliceAsMap>>> iterators) {
			this.queryStep = queryStep;
			sortedIterators = iterators.stream().map(i -> Iterators.peekingIterator(i)).toList();
		}

		@Override
		public boolean hasNext() {
			return sortedIterators.stream().anyMatch(i -> i.hasNext());
		}

		@Override
		public SliceAndMeasures next() {
			List<SliceAndMeasure<SliceAsMap>> sliceAndMeasures = sortedIterators.stream()
					.map(i -> i.hasNext() ? i.peek() : null)
					// Need a mutable List as we'll later replace some index with a
					.collect(Collectors.toCollection(ArrayList::new));

			// As we iterate through sorted slices, we keep the minimum slice amongst candidates
			SliceAsMap slice = sliceAndMeasures.stream()
					.filter(Objects::nonNull)
					.map(SliceAndMeasure::getSlice)
					.min(Comparator.naturalOrder())
					.get();

			Consumer<IValueConsumer> nullValueConsumerConsumer = vc -> vc.onObject(null);

			sliceAndMeasures.stream()
					.map(sm -> sm == null ? nullValueConsumerConsumer : sm.getValueConsumerConsumer())
					.toList();

			int size = sliceAndMeasures.size();
			List<Consumer<IValueConsumer>> valueConsumerConsumers = new ArrayList<>(size);

			for (int i = 0; i < size; i++) {
				SliceAndMeasure<SliceAsMap> sliceAndMeasure = sliceAndMeasures.get(i);
				if (sliceAndMeasure == null || !sliceAndMeasure.getSlice().equals(slice)) {
					// Given peeked elements, this slice is not present in this column
					valueConsumerConsumers.add(nullValueConsumerConsumer);
				} else {
					// Given peeked elements, this slice is confirmed in this column
					sortedIterators.get(i).next();
					valueConsumerConsumers.add(sliceAndMeasures.get(i).getValueConsumerConsumer());
				}
			}

			return SliceAndMeasures.from(queryStep, slice, valueConsumerConsumers);
		}
	}

	private static Stream<SliceAndMeasures> makeSortedStream(AdhocQueryStep queryStep, List<ISliceToValue> sorted) {
		List<Iterator<SliceAndMeasure<SliceAsMap>>> sortedIterators =
				sorted.stream().map(s -> s.stream().iterator()).toList();

		Iterator<SliceAndMeasures> mergedIterator = new MergedSlicesIterator(queryStep, sortedIterators);

		int characteristics = // keys are sorted naturally
				Spliterator.ORDERED | Spliterator.SORTED |
				// When read, this can not be edited anymore
						Spliterator.IMMUTABLE;
		Spliterator<SliceAndMeasures> spliterator =
				Spliterators.spliteratorUnknownSize(mergedIterator, characteristics);
		return StreamSupport.stream(spliterator, false);
	}

}
