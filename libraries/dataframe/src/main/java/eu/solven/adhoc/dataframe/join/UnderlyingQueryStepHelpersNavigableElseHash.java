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
package eu.solven.adhoc.dataframe.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Booleans;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashColumn;
import eu.solven.adhoc.dataframe.column.partitioned.IPartitioned;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.stream.ConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper around {@link ICuboid}.
 * 
 * @author Benoit Lacelle
 * @see MultitypeNavigableElseHashColumn
 */
@Slf4j
@UtilityClass
public class UnderlyingQueryStepHelpersNavigableElseHash {

	/**
	 * Returns distinct slices from the given cuboids. If all cuboids are partitioned with the same partition count,
	 * produces one {@link IConsumingStream} per partition and concatenates them, enabling per-partition parallelism
	 * downstream.
	 * 
	 * @param queryStep
	 *            the considered queryStep.
	 * @param underlyings
	 *            the underlyings for given queryStep.
	 * @return the union-Set of slices
	 */
	public static IConsumingStream<SliceAndMeasures> distinctSlices(CubeQueryStep queryStep,
			List<? extends ICuboid> underlyings) {
		int commonPartitions = commonPartitionCount(underlyings);
		if (commonPartitions > 1) {
			// Process each partition independently and concatenate the results
			List<IConsumingStream<SliceAndMeasures>> perPartition = new ArrayList<>();
			for (int p = 0; p < commonPartitions; p++) {
				int partitionIndex = p;
				List<ICuboid> partitionCuboids = underlyings.stream()
						.map(c -> ((IPartitioned<ICuboid>) c).getPartition(partitionIndex))
						.toList();
				perPartition.add(distinctSlicesAsStream(queryStep, partitionCuboids));
			}
			return IConsumingStream.concat(perPartition);
		} else {
			return distinctSlicesAsStream(queryStep, underlyings);
		}
	}

	/**
	 * @return the common partition count if all cuboids are partitioned with the same count, or -1 otherwise.
	 */
	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	protected static int commonPartitionCount(List<? extends ICuboid> underlyings) {
		int[] distinct = underlyings.stream()
				.mapToInt(c -> c instanceof IPartitioned<?> p ? p.getNbPartitions() : -1)
				.distinct()
				.toArray();

		if (distinct.length == 1 && distinct[0] > 1) {
			// May be `-1`
			return distinct[0];
		} else {
			return -1;
		}
	}

	/**
	 * Internal implementation for {@link #distinctSlices}. Operates on a single set of (possibly partition-scoped)
	 * cuboids and natively returns an {@link IConsumingStream}.
	 */
	private static IConsumingStream<SliceAndMeasures> distinctSlicesAsStream(CubeQueryStep queryStep,
			List<? extends ICuboid> underlyings) {
		boolean debug = queryStep.isDebug();

		if (underlyings.isEmpty()) {
			return IConsumingStream.empty();
		} else if (!debug && underlyings.size() == 1) {
			// Fast track
			ICuboid underlying = underlyings.getFirst();
			return underlying.stream().map(slice -> {
				Object value = IValueProvider.getValue(slice.getValueProvider());

				return SliceAndMeasures.from(slice.getSlice(), queryStep, ImmutableList.of(value));
			});
		}

		IConsumingStream<SliceAndMeasures> sortedSlices = mergeSortedStreamDistinct(queryStep, underlyings);

		boolean[] indexToNotSorted = new boolean[underlyings.size()];

		for (int i = 0; i < underlyings.size(); i++) {
			// TODO We may have count the number of notSorted processed slices, to know if there is no more pending
			// slices
			if (underlyings.get(i).stream(StreamStrategy.SORTED_SUB_COMPLEMENT).findAny().isPresent()) {
				indexToNotSorted[i] = true;
			}
		}

		if (!Booleans.contains(indexToNotSorted, true)) {
			// Not a single notSorted leg
			return sortedSlices;
		}

		// This is expensive: collected all sorted slices, to skip them from notSorted legs
		Set<ISlice> sortedSlicesAsSet = new LinkedHashSet<>();

		// BEWARE: This will fill `sortedSlicesAsSet` lazily, while iterating along sortedSlices, before processing
		// notSorted slices. This relies on IConsumingStream being sequential (push-based).
		sortedSlices = sortedSlices.peek(s -> {
			sortedSlicesAsSet.add(s.getSlice().getSlice());
		});

		List<IConsumingStream<SliceAndMeasures>> unsortedStreams =
				unsortedStreams(queryStep, underlyings, sortedSlicesAsSet);

		// Process ordered slices in priority. Good for MultitypeNavigableElseHashColumn
		// unsortedStreams requires sortedSlices to be consumed first: IConsumingStream.fromStream(List)
		// concatenates sequentially, so this invariant holds.
		List<IConsumingStream<SliceAndMeasures>> allStreams = new ArrayList<>();
		allStreams.add(sortedSlices);
		allStreams.addAll(unsortedStreams);
		return IConsumingStream.concat(allStreams);
	}

	/**
	 * Produces the merged sorted stream from sorted sub-columns of each cuboid.
	 *
	 * @param queryStep
	 *            the considered queryStep.
	 * @param underlyings
	 *            {@link ICuboid} required to be sorted.
	 * @return a merged {@link IConsumingStream}, based on the input sorted ICuboid
	 */
	private static IConsumingStream<SliceAndMeasures> mergeSortedStreamDistinct(CubeQueryStep queryStep,
			List<? extends ICuboid> underlyings) {
		// Exclude the notSortedIterators: they will be processed in a later step
		List<Iterator<SliceAndMeasure<ISlice>>> sortedIterators = underlyings.stream().map(s -> {
			return s.stream(StreamStrategy.SORTED_SUB).toList().iterator();
		}).toList();

		Iterator<SliceAndMeasures> mergedIterator =
				new MergedSlicesIteratorNavigableElseHash(queryStep, sortedIterators, underlyings);

		// Push-based: drain the iterator into the consumer
		return ConsumingStream.<SliceAndMeasures>builder().source(consumer -> {
			while (mergedIterator.hasNext()) {
				consumer.accept(mergedIterator.next());
			}
		}).build();
	}

	/**
	 * Builds one {@link IConsumingStream} per unsorted cuboid, each filtering out slices already seen by the sorted
	 * pass or by previous unsorted cuboids.
	 *
	 * @param queryStep
	 *            the considered queryStep.
	 * @param underlyings
	 *            a List of unsorted {@link ICuboid}
	 * @param sortedSlicesAsSet
	 *            the Set of slices which were already processed by the sorted streams. May be filled lazily, which is
	 *            fine as long as unsorted streams are consumed strictly after the sorted streams.
	 * @return a List of {@link IConsumingStream} handling unsorted columns
	 */
	@SuppressWarnings("PMD.CloseResource")
	private static List<IConsumingStream<SliceAndMeasures>> unsortedStreams(CubeQueryStep queryStep,
			List<? extends ICuboid> underlyings,
			Set<ISlice> sortedSlicesAsSet) {
		Set<ISlice> unsortedSlicesAsSet = ConcurrentHashMap.newKeySet();

		List<IConsumingStream<SliceAndMeasures>> unsortedStreams = new ArrayList<>();

		int size = underlyings.size();

		// Iterate through unsorted stream of slices, considering each of them at a time, and skipping previously
		// considered slices
		for (int rawUnsortedIndex = 0; rawUnsortedIndex < size; rawUnsortedIndex++) {
			int unsortedIndex = rawUnsortedIndex;
			ICuboid cuboid = underlyings.get(unsortedIndex);
			IConsumingStream<SliceAndMeasures> unsortedStream = cuboid.stream(StreamStrategy.SORTED_SUB_COMPLEMENT)
					// Skip the slices already processed by sorted iterators
					.filter(s -> !sortedSlicesAsSet.contains(s.getSlice()))
					// Skip the slices processed by unsorted iterators
					.filter(s -> !unsortedSlicesAsSet.contains(s.getSlice()))
					.map(s -> {
						SliceAndMeasure<ISlice> sliceAndMeasure = s;

						ImmutableList.Builder<IValueProvider> valueProviders =
								ImmutableList.builderWithExpectedSize(size);

						// No point in processing previous columns as their slices are already processed
						for (int ii = 0; ii < unsortedIndex; ii++) {
							valueProviders.add(IValueProvider.NULL);
						}
						for (int ii = unsortedIndex; ii < size; ii++) {
							if (ii == unsortedIndex) {
								// Current column
								valueProviders.add(s.getValueProvider());
							} else {
								ICuboid underlying = underlyings.get(ii);
								if (!underlying.isSorted()) {
									// Only unsorted columns are candidate
									valueProviders.add(underlying.onValue(s.getSlice()));
								} else {
									// sorted columns got all their slices processed previously
									valueProviders.add(IValueProvider.NULL);
								}
							}
						}

						// If this is not the last unsorted stream
						if (unsortedIndex < size - 1) {
							// Prevent this slice to be considered by next unsorted cuboids
							unsortedSlicesAsSet.add(s.getSlice());
						}

						return SliceAndMeasures.from(queryStep, sliceAndMeasure.getSlice(), valueProviders.build());
					});

			unsortedStreams.add(unsortedStream);
		}
		return unsortedStreams;
	}
}
