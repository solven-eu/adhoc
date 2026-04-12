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
package eu.solven.adhoc.dataframe.aggregating;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Suppliers;

import eu.solven.adhoc.collection.AdhocCollectionHelpers;
import eu.solven.adhoc.collection.FrozenException;
import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.collection.IFreezable;
import eu.solven.adhoc.dataframe.IAdhocCapacityConstants;
import eu.solven.adhoc.dataframe.collection.ChunkedList;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.ICubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier.IHasCarriers;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IAliasedAggregator;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.pepper.core.PepperStreamHelper;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A data-structure associating each {@link Aggregator} with a {@link IMultitypeMergeableColumn}
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
@SuppressWarnings("checkstyle:TypeName")
// NotThreadSafe
public class AggregatingColumnsDistinct<T extends Comparable<T>> extends AAggregatingColumns<T, Integer>
		implements IFreezable {
	@NonNull
	@Default
	final IAdhocFactories factories = AdhocFactoriesUnsafe.factories;

	// This dictionarize the slice, with a common dictionary to all aggregators. This is expected to be efficient as, in
	// most cases, all aggregators covers the same slices. But this design enables sorting the slices only once (for all
	// aggregators).
	@NonNull
	@Default
	final List<T> indexToSlice = new ChunkedList<>();

	@NonNull
	@Default
	final Map<String, IMultitypeColumnFastGet<Integer>> aggregatorToAggregates = new LinkedHashMap<>();

	final Supplier<Object2IntFunction<T>> memoizeSliceToIndex = Suppliers.memoize(this::sliceToIndex);

	// Set to true on the first closeColumn() call; subsequent openSlice() calls are rejected.
	// non-final: set lazily on close, not at construction time
	private boolean frozen;

	@Override
	public boolean isFrozen() {
		return frozen;
	}

	// preColumn: we would not need to merge as the DB should guarantee providing distinct aggregates
	// In fact, some DB may provide aggregates, but partitioned: we may receive the same aggregate on the same slice
	// Also, even if we hit a single aggregate, it should not be returned as-is, but returns as aggregated with null
	// given the aggregation. It is typically useful to turn `BigDecimal` from DuckDb into `double`. Another
	// SumAggregation may stick to BigDecimal
	protected IMultitypeColumnFastGet<Integer> makePreColumn() {
		// Sequential dictionarization indices (0, 1, 2, ...) always arrive in ascending order, so the navigable
		// side of the MultitypeNavigableElseHashColumn is always taken — no hash fallback, no 1M-default-capacity
		// allocation from MultitypeHashColumn (whose `ensureCapacity` would otherwise pre-allocate
		// `AdhocColumnUnsafe#getDefaultColumnCapacity()` slots on first write).
		// BEWARE This column ends up "sorted" by Integer index — which is the dictionarization-insertion order,
		// NOT the slice order. Downstream code MUST NOT propagate that ordering as slice-sortedness; the wrapping
		// UndictionarizedColumn carries an explicit `sortedPrefixLength` field for that purpose, populated from
		// AAggregatingColumns#sortedPrefixLength.
		return factories.getColumnFactory().makeColumn(IAdhocCapacityConstants.ZERO_THEN_MAX);
	}

	@Override
	protected IMultitypeColumnFastGet<Integer> getColumn(String aggregator) {
		return aggregatorToAggregates.get(aggregator);
	}

	@Override
	public IOpenedSlice openSlice(T key) {
		if (frozen) {
			throw new FrozenException(
					"AggregatingColumnsDistinct is frozen: openSlice must not be called after closeColumn");
		}
		int keyIndex = dictionarize(key);

		return aggregator -> {
			IMultitypeColumnFastGet<Integer> column =
					aggregatorToAggregates.computeIfAbsent(aggregator.getAlias(), _ -> makePreColumn());

			IAggregation agg = aggregations.get().get(aggregator.getAlias());

			if (agg instanceof IHasCarriers hasCarriers) {
				return hasCarriers.wrap(column.append(keyIndex));
			} else {
				return column.append(keyIndex);
			}
		};
	}

	@Override
	protected int dictionarize(T key) {
		// recordNewSlice expects the new slice not to be in storage
		recordNewSlice(key);
		indexToSlice.add(key);

		// e.g. On first item, we return 0
		return indexToSlice.size() - 1;
	}

	@Override
	protected int sliceCount() {
		return indexToSlice.size();
	}

	@Override
	public IMultitypeColumnFastGet<T> closeColumn(ICubeQueryStep queryStep, IAliasedAggregator aggregator) {
		// Freeze on first closeColumn: all slices have been ingested; further openSlice calls are invalid.
		frozen = true;

		String aggregatorName = aggregator.getAlias();
		IMultitypeColumnFastGet<Integer> column = getColumn(aggregatorName);

		if (column == null) {
			// Typically happens when a filter reject completely one of the underlying
			// measure, and not a single aggregate was written
			column = MultitypeHashColumn.empty();
		}

		if (column.isEmpty()) {
			// RAM optimization
			return MultitypeHashColumn.empty();
		} else if (column instanceof ICompactable compactable) {
			compactable.compact();
		}

		// Turn the columnByIndex to a columnBySlice. Pass the prefix length so the wrapper exposes the leading
		// slice-sorted run via stream(SORTED_SUB) / onValue(slice, SORTED_SUB).
		return AggregatingColumns.undictionarizeColumn(column,
				memoizeSliceToIndex.get(),
				indexToSlice::get,
				getNbSorted(aggregatorName, column));
	}

	protected Object2IntFunction<T> sliceToIndex() {
		AdhocCollectionHelpers.trimToSize(indexToSlice);

		// Reverse from `slice->index` to `index->slice`
		Object2IntMap<T> sliceToIndex = AdhocPrimitiveMapHelpers.newHashMapDefaultMinus1(indexToSlice.size());
		for (int i = 0; i < indexToSlice.size(); i++) {
			sliceToIndex.put(indexToSlice.get(i), i);
		}

		return sliceToIndex::getInt;
	}

	@Override
	public Set<String> getAggregators() {
		return aggregatorToAggregates.keySet();
	}

	@Override
	public String toString() {
		ToStringHelper sh = MoreObjects.toStringHelper(this);

		sh.add("#slices", indexToSlice.size());
		sh.add("aggregators", getAggregators().size());

		IntStream.range(0, indexToSlice.size()).limit(AdhocUnsafe.getLimitOrdinalToString()).forEach(sliceIndex -> {
			Map<String, String> aggregates = aggregatorToAggregates.keySet()
					.stream()
					// `String.valueOf` will cover null values
					.collect(PepperStreamHelper.toLinkedMap(Function.identity(),
							a -> String.valueOf(
									IValueProvider.getValue(aggregatorToAggregates.get(a).onValue(sliceIndex)))));

			sh.add(String.valueOf(indexToSlice.get(sliceIndex)), aggregates);
		});

		return sh.toString();
	}
}