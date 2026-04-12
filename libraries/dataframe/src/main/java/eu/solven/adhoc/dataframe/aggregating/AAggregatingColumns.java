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
package eu.solven.adhoc.dataframe.aggregating;

import java.util.LinkedHashMap;
import java.util.Map;

import eu.solven.adhoc.dataframe.column.IMultitypeColumn;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.navigable.IHasSortedLeg;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.dataframe.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.measure.model.IAliasedAggregator;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Common behavior, given specialization would typically change their behavior depending on if
 * {@link ITabularRecordStream} is distinctSlices or not.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public abstract class AAggregatingColumns<T extends Comparable<T>, K> implements IMultitypeMergeableGrid<T> {

	@NonNull
	@Default
	protected IOperatorFactory operatorFactory = StandardOperatorFactory.builder().build();

	/**
	 * Number of leading dictionarization indices for which the corresponding slices were inserted in strictly
	 * increasing slice order. Once a non-monotonic insertion happens this freezes — it never grows again.
	 * <p>
	 * Subclasses populate this via {@link #recordNewSlice(Comparable)} and downstream code (typically
	 * {@code UndictionarizedColumn}) reads it via {@link #getSortedPrefixLength()} to expose proper sorted-leg
	 * semantics on the closed column.
	 */
	// protected int sortedPrefixLength;
	protected final Map<String, Long> aggregatorToSortedLength = new LinkedHashMap<>();

	/**
	 * Last element of the still-extending sorted prefix. {@code null} until the first insert AND once the prefix has
	 * frozen — kept only while we may still extend it. Releasing the reference on freeze avoids retaining the slice
	 * longer than needed.
	 */
	protected T lastSortedKey;

	protected abstract int dictionarize(T key);

	protected abstract IMultitypeColumn<K> getColumn(String aggregator);

	/**
	 * Subclasses MUST call this exactly once for every newly-inserted slice, <strong>after</strong> the new
	 * dictionarization index has been allocated (so {@link #sliceCount()} reflects the post-insertion count and the new
	 * slice lives at index {@code sliceCount() - 1}). Existing-slice paths (e.g. {@code computeIfAbsent} hitting an
	 * already-known key) MUST NOT call this — re-visits do not advance the prefix.
	 *
	 * @param newSlice
	 *            the slice that was just inserted, now at index {@code sliceCount() - 1}
	 */
	@SuppressWarnings("PMD.NullAssignment")
	protected void recordNewSlice(T newSlice) {
		if (!aggregatorToSortedLength.isEmpty()) {
			// notSortedAnymore event already happened
			return;
		}

		if (lastSortedKey == null || lastSortedKey.compareTo(newSlice) < 0) {
			// sortedPrefixLength++;
			lastSortedKey = newSlice;
		} else {
			// Break — release the reference, the prefix is now frozen.
			lastSortedKey = null;

			assert !getAggregators().isEmpty();
			// Each column may have a difference size at the notSortedAnymore event
			getAggregators().forEach(aggregator -> {
				IMultitypeColumn<K> column = getColumn(aggregator);

				aggregatorToSortedLength.put(aggregator, getSortedLength(column));
			});
		}
	}

	protected long getSortedLength(IMultitypeColumn<K> column) {
		long sortedLength;
		if (column instanceof IHasSortedLeg hasSortedLeg) {
			// In AggregatingColumns, as we can encounter multiple times given slice, the initial encountering may be
			// ordered, while given aggregator did not encounter in sorter order (e.g. `a1->k1;a2->k2;a1->k2`, then k2
			// encountered a2 before a1, and a1 has only 1 sorted elements).

			sortedLength = hasSortedLeg.sortedPrefixLength();
		} else {
			sortedLength = 0L;
		}
		return sortedLength;
	}

	/**
	 * @return the current dictionarization size <strong>after</strong> the most recent insertion. Used by
	 *         {@link #recordNewSlice} to compute the index of the slice just inserted.
	 */
	protected abstract int sliceCount();

	/**
	 * @return the longest leading run of slices observed in strictly increasing slice order. Stable once the prefix has
	 *         frozen.
	 */
	protected long getNbSorted(String aggregatorName, IMultitypeColumnFastGet<K> column) {
		if (!aggregatorToSortedLength.isEmpty()) {
			// If the aggregator were not snapshot, it means we did not record any slice when the notSortedAnymore event
			// occured
			return aggregatorToSortedLength.getOrDefault(aggregatorName, 0L);
		} else {
			// All slices are sorted
			return getSortedLength(column);
		}
	}

	protected IMultitypeColumn<K> getColumn(IAliasedAggregator aggregator) {
		return getColumn(aggregator.getAlias());
	}

	@Override
	public long size(String aggregator) {
		long size = 0L;

		IMultitypeColumn<?> preColumn = getColumn(aggregator);
		if (preColumn != null) {
			size += preColumn.size();
		}

		return size;
	}
}
