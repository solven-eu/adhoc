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
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashColumn;
import eu.solven.adhoc.dataframe.column.partitioned.IPartitioned;
import eu.solven.adhoc.dataframe.column.partitioned.PartitioningHelpers;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.stream.ConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;
import eu.solven.adhoc.stream.partitioned.PartitionedConsumingStream;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
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
		OptionalInt commonPartitions = PartitioningHelpers.commonPartitionCount(underlyings);
		if (commonPartitions.isPresent()) {
			// Process each partition independently and concatenate the results
			List<IConsumingStream<SliceAndMeasures>> perPartition = new ArrayList<>();
			for (int p = 0; p < commonPartitions.getAsInt(); p++) {
				int partitionIndex = p;
				List<ICuboid> partitionCuboids = underlyings.stream()
						.map(c -> ((IPartitioned<ICuboid>) c).getPartition(partitionIndex))
						.toList();
				perPartition.add(distinctSlicesAsStream(queryStep, partitionCuboids));
			}
			return PartitionedConsumingStream.<SliceAndMeasures>builder().partitions(perPartition).build();
		} else {
			return distinctSlicesAsStream(queryStep, underlyings);
		}
	}

	/**
	 * Pre-computed per-cuboid data: the lazy unsorted stream (from {@code stream(SORTED_SUB_COMPLEMENT)}, obtained
	 * exactly once), and the value-lookup function for the unsorted pass.
	 *
	 * <p>
	 * {@code valueLookup} returns {@link IValueProvider#NULL} for fully-sorted cuboids (whose unsorted stream is
	 * empty), and delegates to {@code cuboid::onValue} otherwise.
	 *
	 * @param unsortedStream
	 *            the lazy stream of unsorted slices — consumed at most once during the unsorted pass
	 * @param valueLookup
	 *            maps a slice to its value provider for this cuboid
	 */
	private record CuboidUnsortedLeg(IConsumingStream<SliceAndMeasure<ISlice>> unsortedStream,
			Function<ISlice, IValueProvider> valueLookup) {
	}

	/**
	 * Builds the unsorted leg for each cuboid, calling {@code stream(SORTED_SUB_COMPLEMENT)} exactly once per cuboid.
	 * The stream remains lazy — it is consumed later by {@link #unsortedStreams}.
	 */
	@SuppressWarnings("PMD.CloseResource")
	private static List<CuboidUnsortedLeg> buildUnsortedLegs(List<? extends ICuboid> underlyings) {
		List<CuboidUnsortedLeg> legs = new ArrayList<>(underlyings.size());
		for (ICuboid cuboid : underlyings) {
			IConsumingStream<SliceAndMeasure<ISlice>> unsorted = cuboid.stream(StreamStrategy.SORTED_SUB_COMPLEMENT);
			// We cannot know cheaply if the stream is empty without consuming it.
			// Assume it may have content; empty streams simply produce nothing when consumed.
			legs.add(new CuboidUnsortedLeg(unsorted,
					slice -> cuboid.onValue(slice, StreamStrategy.SORTED_SUB_COMPLEMENT)));
		}
		return legs;
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
				ImmutableList<IValueProvider> valueProviders = ImmutableList.of(slice.getValueProvider());
				return SliceAndMeasures.fromProviders(queryStep, slice.getSlice(), valueProviders);
			});
		}

		IConsumingStream<SliceAndMeasures> sortedSlicesStream = mergeSortedStreamDistinct(queryStep, underlyings);

		// Build unsorted legs once — each cuboid's stream(SORTED_SUB_COMPLEMENT) is called exactly once
		List<CuboidUnsortedLeg> unsortedLegs = buildUnsortedLegs(underlyings);

		// Collect sorted slices lazily, so unsorted streams can skip already-processed slices
		Set<ISlice> sortedSlices = new LinkedHashSet<>();

		// BEWARE: This will fill `sortedSlicesAsSet` lazily, while iterating along sortedSlices, before processing
		// unsorted slices. This relies on IConsumingStream being sequential (push-based).
		sortedSlicesStream = sortedSlicesStream.peek(s -> sortedSlices.add(s.getSlice().getSlice()));

		List<IConsumingStream<SliceAndMeasures>> unsortedStreams =
				unsortedStreams(queryStep, unsortedLegs, sortedSlices);

		// Process ordered slices in priority. Good for MultitypeNavigableElseHashColumn
		// unsortedStreams requires sortedSlices to be consumed first: IConsumingStream.concat(List)
		// concatenates sequentially, so this invariant holds.
		return IConsumingStream.concat(ImmutableList.<IConsumingStream<SliceAndMeasures>>builder()
				.add(sortedSlicesStream)
				.addAll(unsortedStreams)
				.build());
	}

	/**
	 * Produces the merged sorted stream from sorted sub-columns of each cuboid.
	 *
	 * @param queryStep
	 *            the considered queryStep.
	 * @param cuboids
	 *            {@link ICuboid} required to be sorted.
	 * @return a merged {@link IConsumingStream}, based on the input sorted ICuboid
	 */
	private static IConsumingStream<SliceAndMeasures> mergeSortedStreamDistinct(CubeQueryStep queryStep,
			List<? extends ICuboid> cuboids) {
		// Exclude the notSortedIterators: they will be processed in a later step
		List<Iterator<SliceAndMeasure<ISlice>>> sortedIterators = cuboids.stream().map(cuboid -> {
			return cuboid.stream(StreamStrategy.SORTED_SUB).toList().iterator();
		}).toList();

		Iterator<SliceAndMeasures> mergedIterator =
				new MergedSlicesIteratorNavigableElseHash(queryStep, sortedIterators, cuboids);

		// Push-based: drain the iterator into the consumer
		return ConsumingStream.<SliceAndMeasures>builder().source(mergedIterator::forEachRemaining).build();
	}

	/**
	 * Builds one {@link IConsumingStream} per unsorted leg, each filtering out slices already seen by the sorted pass
	 * or by previous unsorted legs.
	 *
	 * <p>
	 * BEWARE The output has to be processed sequentially as it relies on state built by iteration (the slices already
	 * processed).
	 *
	 * @param queryStep
	 *            the considered queryStep.
	 * @param unsortedLegs
	 *            pre-computed unsorted data for each cuboid
	 * @param sortedSlicesAsSet
	 *            the Set of slices which were already processed by the sorted streams. May be filled lazily, which is
	 *            fine as long as unsorted streams are consumed strictly after the sorted streams.
	 * @return a List of {@link IConsumingStream} handling unsorted columns
	 */
	@SuppressWarnings("PMD.CloseResource")
	private static List<IConsumingStream<SliceAndMeasures>> unsortedStreams(CubeQueryStep queryStep,
			List<CuboidUnsortedLeg> unsortedLegs,
			Set<ISlice> sortedSlicesAsSet) {
		// Use a FastUtil expected to have better performance than HashSet
		Set<ISlice> unsortedSlicesAsSet = new ObjectOpenHashSet<>();

		int size = unsortedLegs.size();
		List<IConsumingStream<SliceAndMeasures>> unsortedStreams = new ArrayList<>(size);

		for (int rawUnsortedIndex = 0; rawUnsortedIndex < size; rawUnsortedIndex++) {
			int unsortedIndex = rawUnsortedIndex;
			CuboidUnsortedLeg leg = unsortedLegs.get(unsortedIndex);
			Set<ISlice> unsortedSlicesAsSetI = new ObjectOpenHashSet<>();

			// The stream may be empty for fully-sorted cuboids — that's fine, it simply produces nothing
			IConsumingStream<SliceAndMeasures> unsortedStream = leg.unsortedStream()
					// Skip the slices already processed by sorted iterators
					.filter(s -> !sortedSlicesAsSet.contains(s.getSlice()))
					// Skip the slices processed by previous unsorted iterators
					.filter(s -> !unsortedSlicesAsSet.contains(s.getSlice()))
					.map(sliceAndMeasure -> {
						List<IValueProvider> valueProviders =
								makeValueProviders(unsortedLegs, unsortedIndex, sliceAndMeasure);

						// If this is not the last unsorted stream, prevent this slice from being
						// considered by next unsorted cuboids
						if (unsortedIndex < size - 1) {
							// TODO Write into an intermediate Set not to pollute current iteration
							unsortedSlicesAsSetI.add(sliceAndMeasure.getSlice());
						}

						return SliceAndMeasures.fromProviders(queryStep, sliceAndMeasure.getSlice(), valueProviders);
					})
					// Registration of encountered slices needs to be delayed on close-time
					.onClose(() -> unsortedSlicesAsSet.addAll(unsortedSlicesAsSetI));
			unsortedStreams.add(unsortedStream);
		}
		return unsortedStreams;
	}

	/**
	 * Builds the value-provider list for a given slice across all cuboids. Uses each leg's pre-computed
	 * {@link CuboidUnsortedLeg#valueLookup()} — returns {@link IValueProvider#NULL} for legs with no unsorted content
	 * (i.e. fully sorted columns whose slices were already processed).
	 */
	private static List<IValueProvider> makeValueProviders(List<CuboidUnsortedLeg> unsortedLegs,
			int unsortedIndex,
			SliceAndMeasure<ISlice> sliceAndMeasure) {
		int size = unsortedLegs.size();
		ImmutableList.Builder<IValueProvider> valueProviders = ImmutableList.builderWithExpectedSize(size);

		// Previous columns' slices are already processed
		for (int ii = 0; ii < unsortedIndex; ii++) {
			valueProviders.add(IValueProvider.NULL);
		}
		for (int ii = unsortedIndex; ii < size; ii++) {
			if (ii == unsortedIndex) {
				// Current column — use the value from the slice itself
				valueProviders.add(sliceAndMeasure.getValueProvider());
			} else {
				// Other columns — valueLookup returns NULL for fully-sorted legs, onValue otherwise
				valueProviders.add(unsortedLegs.get(ii).valueLookup().apply(sliceAndMeasure.getSlice()));
			}
		}
		return valueProviders.build();
	}
}
