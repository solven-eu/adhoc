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
package eu.solven.adhoc.data.tabular;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.solven.adhoc.data.column.ICompactable;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier.IHasCarriers;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.table.IAliasedAggregator;
import eu.solven.adhoc.util.AdhocUnsafe;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
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
public class AggregatingColumns<T extends Comparable<T>> extends AAggregatingColumns<T, Integer> {

	@NonNull
	@Default
	AdhocFactories factories = AdhocFactories.builder().build();

	// May go for Hash or Navigable
	// This dictionarize the slice, with a common dictionary to all aggregators. This is expected to be efficient as, in
	// most cases, all aggregators covers the same slices. But this design enables sorting the slices only once (for all
	// aggregators).
	// https://questdb.com/blog/building-faster-hash-table-high-performance-sql-joins/
	@NonNull
	@Default
	Object2IntMap<T> sliceToIndex = newHashMapDefaultMinus1();

	@NonNull
	@Default
	Map<String, IMultitypeMergeableColumn<Integer>> aggregatorToAggregates = new LinkedHashMap<>();

	@SuppressWarnings("PMD.LooseCoupling")
	private static <T> Object2IntMap<T> newHashMapDefaultMinus1() {
		Object2IntOpenHashMap<T> map = new Object2IntOpenHashMap<>(AdhocUnsafe.getDefaultColumnCapacity());

		// If we request an unknown slice, we must not map to an existing index
		map.defaultReturnValue(-1);

		return map;
	}

	// preColumn: we would not need to merge as the DB should guarantee providing distinct aggregates
	// In fact, some DB may provide aggregates, but partitioned: we may receive the same aggregate on the same slice
	// Also, even if we hit a single aggregate, it should not be returned as-is, but returns as aggregated with null
	// given the aggregation. It is typically useful to turn `BigDecimal` from DuckDb into `double`. Another
	// SumAggregation may stick to BigDecimal
	protected IMultitypeMergeableColumn<Integer> makePreColumn(IAggregation agg) {
		// Not all table will provide slices properly sorted (e.g. InMemoryTable)
		return factories.getColumnsFactory().makeColumn(agg, Arrays.asList());
	}

	@Override
	protected IMultitypeMergeableColumn<Integer> getColumn(IAliasedAggregator aggregator) {
		return aggregatorToAggregates.get(aggregator.getAlias());
	}

	@Override
	public IOpenedSlice openSlice(T key) {
		int keyIndex = dictionarize(key);
		return aggregator -> {
			IMultitypeMergeableColumn<Integer> column =
					aggregatorToAggregates.computeIfAbsent(aggregator.getAlias(), k -> {
						IAggregation agg = operatorFactory.makeAggregation(aggregator.getAggregator());
						return makePreColumn(agg);
					});

			if (column.getAggregation() instanceof IHasCarriers hasCarriers) {
				return hasCarriers.wrap(column.append(keyIndex));

			} else {
				return column.append(keyIndex);
			}
		};
	}

	@Override
	protected int dictionarize(T key) {
		// Dictionarize the slice to some index.
		// BEWARE This happens even if the aggregates is null and then should be skipped,
		// which is sub-optimal (but IValueReceiver API leads to this). It is acceptable as we expect at least one
		// aggregate to have a value for given slice.
		return sliceToIndex.computeIfAbsent(key, k -> sliceToIndex.size());
	}

	@Override
	public IMultitypeColumnFastGet<T> closeColumn(IAliasedAggregator aggregator
	// , boolean purgeCarriers
	) {
		IMultitypeColumnFastGet<Integer> notFinalColumn = getColumn(aggregator);

		if (notFinalColumn == null) {
			// Typically happens when a filter reject completely one of the underlying
			// measure, and not a single aggregate was written
			notFinalColumn = MultitypeHashColumn.empty();
		}
		// else if (purgeCarriers) {
		// // Typically converts a CountHolder into the count as a `long`
		// // May be skipped if the caller is a CompositeCube, requiring to receive the carriers to merge them itself
		// notFinalColumn.purgeAggregationCarriers();
		// }

		if (notFinalColumn instanceof ICompactable compactable) {
			compactable.compact();
		}

		IMultitypeColumnFastGet<Integer> column = notFinalColumn;

		// Reverse from `slice->index` to `index->slice`
		Int2ObjectMap<T> indexToSlice = new Int2ObjectOpenHashMap<>(sliceToIndex.size());
		sliceToIndex.object2IntEntrySet().forEach((e) -> indexToSlice.put(e.getIntValue(), e.getKey()));

		// Turn the columnByIndex to a columnBySlice
		return undictionarizeColumn(column, sliceToIndex::getInt, indexToSlice::get);
	}

	protected static <T> IMultitypeColumnFastGet<T> undictionarizeColumn(IMultitypeColumnFastGet<Integer> column,
			Object2IntFunction<T> sliceToIndex,
			Int2ObjectFunction<T> indexToSlice) {
		return UndictionarizedColumn.<T>builder()
				.indexToSlice(indexToSlice)
				.sliceToIndex(sliceToIndex)
				.column(column)
				.build();
	}
}