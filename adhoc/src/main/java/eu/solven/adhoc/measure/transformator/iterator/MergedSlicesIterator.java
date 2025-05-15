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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;

/**
 * A merging {@link Iterator}, given a {@link List} of sorted {@link Iterator}s.
 * 
 * Similar to Guava {@link Iterators#mergeSorted(Iterable, Comparator)} but preventing duplicates.
 */
public class MergedSlicesIterator extends UnmodifiableIterator<SliceAndMeasures> {

	final CubeQueryStep queryStep;

	final List<PeekingIterator<SliceAndMeasure<SliceAsMap>>> sortedIterators;

	// Used to get faster the next/minimum slice
	final Queue<PeekingIterator<SliceAndMeasure<SliceAsMap>>> queue;

	public MergedSlicesIterator(CubeQueryStep queryStep,
			List<? extends Iterator<SliceAndMeasure<SliceAsMap>>> iterators) {
		this.queryStep = queryStep;
		sortedIterators = iterators.stream().map(Iterators::peekingIterator).toList();

		Comparator<PeekingIterator<SliceAndMeasure<SliceAsMap>>> heapComparator =
				Comparator.comparing(o -> o.peek().getSlice());

		queue = new PriorityQueue<>(sortedIterators.size(), heapComparator);

		sortedIterators.stream()
				// We may receive iterators empty (e.g. an empty queryStep)
				.filter(Iterator::hasNext)
				.forEach(queue::add);
	}

	@Override
	public boolean hasNext() {
		return !queue.isEmpty();
	}

	@Override
	public SliceAndMeasures next() {
		// Peek the nextIterator, i.e. one of the iterator with the minimum slice
		PeekingIterator<SliceAndMeasure<SliceAsMap>> nextIter = queue.peek();
		if (nextIter == null) {
			throw new IllegalStateException(
					"`hasNext` should have returned false as only not-empty iterators are in the queue");
		}

		SliceAsMap slice = nextIter.peek().getSlice();

		int size = sortedIterators.size();

		List<IValueProvider> valueProviders = new ArrayList<>(size);

		// Register how many iterators matches the slice so we can pop them without re-checking their slice
		int nbMatchingSlice = 0;

		for (int i = 0; i < size; i++) {
			PeekingIterator<SliceAndMeasure<SliceAsMap>> iterator = sortedIterators.get(i);
			if (iterator.hasNext() && iterator.peek().getSlice().equals(slice)) {
				// Given peeked elements, this slice is confirmed in this column
				// We could assert it is equal to `slice`
				SliceAndMeasure<SliceAsMap> next = iterator.peek();
				valueProviders.add(next.getValueProvider());

				nbMatchingSlice++;
			} else {
				// Given peeked elements, this slice is not present in this column
				valueProviders.add(IValueProvider.NULL);
			}
		}

		if (nbMatchingSlice == 0) {
			throw new IllegalStateException("nbMatchingSlice should be >0");
		}

		// This array buffers the iterators to insert back, in order not to insert them before doing all removals
		// It is relevant as the queue may hold 2 iterators on same value: we do not want to insert the first iterator
		// on next value while the second iterator
		// is still present with previous value
		List<PeekingIterator<SliceAndMeasure<SliceAsMap>>> insertBack = new ArrayList<>(nbMatchingSlice);

		for (int i = 0; i < nbMatchingSlice; i++) {
			// The first removal is guaranteed as the queue head led to this iteration
			PeekingIterator<SliceAndMeasure<SliceAsMap>> removed = queue.remove();
			// Do the iteration (we only peeked up to now)
			removed.next();

			if (removed.hasNext()) {
				// Insert back in the priority queue
				insertBack.add(removed);
			}
		}

		queue.addAll(insertBack);

		return SliceAndMeasures.from(queryStep, slice, valueProviders);
	}
}