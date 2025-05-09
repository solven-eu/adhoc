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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IColumnValueConverter;
import eu.solven.adhoc.data.column.IMultitypeColumn;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeConstants;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.MultitypeArray;
import eu.solven.adhoc.data.column.MultitypeArray.MultitypeArrayBuilder;
import eu.solven.adhoc.data.column.MultitypeHashColumn;
import eu.solven.adhoc.data.column.MultitypeHashMergeableColumn;
import eu.solven.adhoc.data.column.MultitypeNavigableColumn;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorsFactory;
import eu.solven.adhoc.measure.sum.IAggregationCarrier.IHasCarriers;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.query.table.IAliasedAggregator;
import eu.solven.adhoc.util.AdhocUnsafe;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.AbstractObject2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * A data-structure associating each {@link Aggregator} with a {@link IMultitypeMergeableColumn}
 * 
 * @param <T>
 */
@Value
@Builder
public class AggregatingColumnsV2<T extends Comparable<T>> implements IMultitypeMergeableGrid<T> {
	// May go for Hash or Navigable
	// This dictionarize the slice, with a common dictionary to all aggregators. This is expected to be efficient as, in
	// most cases, all aggregators covers the same slices. But this design enables sorting the slices only once (for all
	// aggregators).
	@NonNull
	@Default
	Object2IntMap<T> sliceToIndex = new Object2IntOpenHashMap<>(AdhocUnsafe.defaultCapacity());

	@NonNull
	@Default
	Map<String, IMultitypeMergeableColumn<Integer>> aggregatorToAggregates = new LinkedHashMap<>();

	@NonNull
	@Default
	IOperatorsFactory operatorsFactory = new StandardOperatorsFactory();

	// Compute only was the sorted version of slicesToIndex. This is re-use by each column
	private final AtomicReference<List<Map.Entry<T, Integer>>> refSorted = new AtomicReference<>();

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
	public IValueReceiver contribute(IAliasedAggregator aggregator, T key) {

		IMultitypeMergeableColumn<Integer> column = aggregatorToAggregates.computeIfAbsent(aggregator.getAlias(), k -> {
			IAggregation agg = operatorsFactory.makeAggregation(aggregator.getAggregator());
			return makePreColumn(agg);
		});

		// Dictionarize the slice to some index.
		// BEWARE This happens even if the aggregates is null and then should be skipped,
		// which is sub-optimal (but IValueReceiver API leads to this). It is acceptable as we expect at least one
		// aggregate to have a value for given slice.
		int keyIndex = sliceToIndex.computeIfAbsent(key, k -> sliceToIndex.size());

		if (column.getAggregation() instanceof IHasCarriers hasCarriers) {
			return v -> {
				Object wrapped;
				if (v == null) {
					wrapped = null;
				} else {
					// Wrap the aggregate from table into the aggregation custom wrapper
					wrapped = hasCarriers.wrap(v);
				}

				column.append(keyIndex).onObject(wrapped);
			};

		} else {
			return column.append(keyIndex);
		}
	}

	@Override
	public IMultitypeColumnFastGet<T> closeColumn(IAliasedAggregator aggregator, boolean purgeCarriers) {
		IMultitypeColumnFastGet<Integer> notFinalColumn = aggregatorToAggregates.get(aggregator.getAlias());

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

			if (refSorted.get() == null) {
				// Do not rely on a TreeMap, else the sorting is done one element at a time
				// ObjectArrayList enables calling `Arrays.parallelSort`
				// `.wrap` else will rely on a `Object[]`, which will later fail on `Arrays.parallelSort`
				ObjectArrayList<Map.Entry<T, Integer>> sortedEntries =
						ObjectArrayList.wrap(new Map.Entry[sliceToIndex.size()], 0);

				sliceToIndex.forEach((slice, rowIndex) -> sortedEntries
						.add(new AbstractObject2IntMap.BasicEntry<>(slice, rowIndex)));

				Arrays.parallelSort(sortedEntries.elements(), Map.Entry.comparingByKey());

				refSorted.set(sortedEntries);
			}

			return copyToNavigable(column, refSorted.get());
		}
	}

	private static <T extends Comparable<T>> IMultitypeColumnFastGet<T> copyToNavigable(
			IMultitypeColumnFastGet<Integer> column,
			List<Map.Entry<T, Integer>> sortedEntries) {
		if (column.isEmpty()) {
			return MultitypeNavigableColumn.empty();
		}

		List<T> slices = new ArrayList<>();
		List<Object> valuesO = new ArrayList<>();

		AtomicLong nbLong = new AtomicLong();
		AtomicLong nbDouble = new AtomicLong();

		sortedEntries.forEach(e -> {
			T slice = e.getKey();
			int rowIndex = e.getValue();

			column.onValue(rowIndex).acceptReceiver(new IValueReceiver() {
				@Override
				public void onLong(long v) {
					nbLong.incrementAndGet();
					slices.add(slice);
					valuesO.add(v);
				}

				@Override
				public void onDouble(double v) {
					nbDouble.incrementAndGet();
					slices.add(slice);
					valuesO.add(v);
				}

				@Override
				public void onObject(Object v) {
					if (v != null) {
						slices.add(slice);
						valuesO.add(v);
					}
				}
			});
		});

		MultitypeArrayBuilder multitypeArrayBuilder = MultitypeArray.builder();
		int size = Ints.checkedCast(column.size());
		if (nbLong.get() == size) {
			final LongArrayList values = new LongArrayList(size);
			valuesO.forEach(o -> values.add((long) o));
			multitypeArrayBuilder.valuesL(values).valuesType(IMultitypeConstants.MASK_LONG);
		} else if (nbDouble.get() == size) {
			final DoubleArrayList values = new DoubleArrayList(size);
			valuesO.forEach(o -> values.add((double) o));
			multitypeArrayBuilder.valuesD(values).valuesType(IMultitypeConstants.MASK_DOUBLE);
		} else {
			multitypeArrayBuilder.valuesO(valuesO).valuesType(IMultitypeConstants.MASK_OBJECT);
		}

		return MultitypeNavigableColumn.<T>builder()
				.keys(slices)
				.values(multitypeArrayBuilder.build())
				.locked(true)
				.build();
	}

	private IMultitypeColumnFastGet<T> wrapNavigableColumnGivenIndexes(IMultitypeColumnFastGet<Integer> column,
			Map<Integer, T> indexToSlice) {
		return new IMultitypeColumnFastGet<T>() {

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
				return column.keyStream().map(rowIndex -> indexToSlice.get(rowIndex));
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

	@Override
	public long size(IAliasedAggregator aggregator) {
		long size = 0L;

		IMultitypeColumn<?> preColumn = aggregatorToAggregates.get(aggregator.getAlias());
		if (preColumn != null) {
			size += preColumn.size();
		}

		return size;
	}
}