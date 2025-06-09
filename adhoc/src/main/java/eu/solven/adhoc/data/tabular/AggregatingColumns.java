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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IColumnValueConverter;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.MultitypeHashColumn;
import eu.solven.adhoc.data.column.MultitypeHashMergeableColumn;
import eu.solven.adhoc.data.column.MultitypeNavigableColumn;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier.IHasCarriers;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.query.table.IAliasedAggregator;
import eu.solven.adhoc.util.AdhocUnsafe;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
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
	// May go for Hash or Navigable
	// This dictionarize the slice, with a common dictionary to all aggregators. This is expected to be efficient as, in
	// most cases, all aggregators covers the same slices. But this design enables sorting the slices only once (for all
	// aggregators).
	// https://questdb.com/blog/building-faster-hash-table-high-performance-sql-joins/
	@NonNull
	@Default
	Object2IntMap<T> sliceToIndex = new Object2IntOpenHashMap<>(AdhocUnsafe.defaultCapacity());

	@NonNull
	@Default
	Map<String, IMultitypeMergeableColumn<Integer>> aggregatorToAggregates = new LinkedHashMap<>();

	// Compute only was the sorted version of slicesToIndex. This is re-used by each column
	private final AtomicReference<List<Object2IntMap.Entry<T>>> refSortedSliceToIndex = new AtomicReference<>();

	// preColumn: we would not need to merge as the DB should guarantee providing distinct aggregates
	// In fact, some DB may provide aggregates, but partitioned: we may receive the same aggregate on the same slice
	// Also, even if we hit a single aggregate, it should not be returned as-is, but returns as aggregated with null
	// given the aggregation. It is typically useful to turn `BigDecimal` from DuckDb into `double`. Another
	// SumAggregation may stick to BigDecimal
	protected IMultitypeMergeableColumn<Integer> makePreColumn(IAggregation agg) {
		// Not Navigable as not all table will provide slices properly sorted (e.g. InMemoryTable)
		// TODO WHy not Navigable as we always finish by sorting in .closeColumn?
		return MultitypeHashMergeableColumn.<Integer>builder().aggregation(agg).build();
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
	public IMultitypeColumnFastGet<T> closeColumn(IAliasedAggregator aggregator, boolean purgeCarriers) {
		IMultitypeColumnFastGet<Integer> notFinalColumn = getColumn(aggregator);

		if (notFinalColumn == null) {
			// Typically happens when a filter reject completely one of the underlying
			// measure, and not a single aggregate was written
			notFinalColumn = MultitypeHashColumn.empty();
		} else if (purgeCarriers) {
			// Typically converts a CountHolder into the count as a `long`
			// May be skipped if the caller is a CompositeCube, requiring to receive the carriers to merge them itself
			notFinalColumn.purgeAggregationCarriers();
		}

		IMultitypeColumnFastGet<Integer> column = notFinalColumn;

		if (column instanceof MultitypeNavigableColumn<?>) {
			Map<Integer, T> indexToSlice = new HashMap<>(sliceToIndex.size());
			sliceToIndex.forEach((slice, rowIndex) -> indexToSlice.put(rowIndex, slice));

			// Turn the columnByIndex to a columnBySlice
			return wrapNavigableColumnGivenIndexes(column, indexToSlice);
		} else {
			// TODO Should we have some sort of strategy to decide when to switch to the sorting strategy?

			// BEWARE The point of this sorting is to improve performance of later
			// UnderlyingQueryStepHelpers.distinctSlices

			if (refSortedSliceToIndex.get() == null) {
				ObjectList<Object2IntMap.Entry<T>> sortedEntries = doSort(consumer -> {
					sliceToIndex.forEach(consumer::acceptObject2Int);
				}, sliceToIndex.size());

				refSortedSliceToIndex.set(sortedEntries);
			}

			return copyToNavigable(column, sliceToIndex -> {
				refSortedSliceToIndex.get().forEach(e -> {
					T slice = e.getKey();
					int rowIndex = e.getIntValue();

					sliceToIndex.acceptObject2Int(slice, rowIndex);
				});
			});
		}
	}

	protected IMultitypeColumnFastGet<T> wrapNavigableColumnGivenIndexes(IMultitypeColumnFastGet<Integer> column,
			Map<Integer, T> indexToSlice) {
		return new IMultitypeColumnFastGet<>() {

			@Override
			public long size() {
				return column.size();
			}

			@Override
			public boolean isEmpty() {
				return column.isEmpty();
			}

			@Override
			public void purgeAggregationCarriers() {
				column.purgeAggregationCarriers();
			}

			@Override
			public IValueReceiver append(T slice) {
				throw new UnsupportedOperationException("Read-Only");
			}

			@Override
			public void scan(IColumnScanner<T> rowScanner) {
				column.scan(rowIndex -> {
					return rowScanner.onKey(indexToSlice.get(rowIndex));
				});
			}

			@Override
			public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
				return column.stream(rowIndex -> {
					return converter.prepare(indexToSlice.get(rowIndex));
				});
			}

			@Override
			public Stream<SliceAndMeasure<T>> stream() {
				return column.stream()
						.map(sliceAndMeasure -> SliceAndMeasure.<T>builder()
								.slice(indexToSlice.get(sliceAndMeasure.getSlice()))
								.valueProvider(sliceAndMeasure.getValueProvider())
								.build());
			}

			@Override
			public Stream<T> keyStream() {
				return column.keyStream().map(indexToSlice::get);
			}

			@Override
			public IValueProvider onValue(T key) {
				return column.onValue(sliceToIndex.get(key));
			}

			@Override
			public IValueReceiver set(T key) {
				throw new UnsupportedOperationException("Read-Only");
			}
		};
	}
}