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
package eu.solven.adhoc.data.tabular;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IMultitypeColumn;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeConstants;
import eu.solven.adhoc.data.column.MultitypeArray;
import eu.solven.adhoc.data.column.MultitypeArray.MultitypeArrayBuilder;
import eu.solven.adhoc.data.column.MultitypeNavigableColumn;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.tabular.primitives.Object2DoubleBiConsumer;
import eu.solven.adhoc.data.tabular.primitives.Object2IntBiConsumer;
import eu.solven.adhoc.data.tabular.primitives.Object2LongBiConsumer;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.query.table.IAliasedAggregator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.AbstractObject2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
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
	IOperatorFactory operatorFactory = new StandardOperatorFactory();

	protected abstract int dictionarize(T key);

	protected abstract IMultitypeColumn<K> getColumn(IAliasedAggregator aggregator);

	@Override
	public long size(IAliasedAggregator aggregator) {
		long size = 0L;

		IMultitypeColumn<?> preColumn = getColumn(aggregator);
		if (preColumn != null) {
			size += preColumn.size();
		}

		return size;
	}

	// visible for benchmarks
	@SuppressWarnings("PMD.LooseCoupling")
	public static <T extends Comparable<T>> ObjectArrayList<Object2IntMap.Entry<T>> doSort(
			Consumer<Object2IntBiConsumer<T>> sliceToIndex,
			int size) {
		log.debug("> sorting {}", size);

		// Do not rely on a TreeMap, else the sorting is done one element at a time
		// ObjectArrayList enables calling `Arrays.parallelSort`
		// `.wrap` else will rely on a `Object[]`, which will later fail on `Arrays.parallelSort`
		ObjectArrayList<Object2IntMap.Entry<T>> sortedEntries = ObjectArrayList.wrap(new Object2IntMap.Entry[size], 0);

		sliceToIndex.accept(
				(slice, rowIndex) -> sortedEntries.add(new AbstractObject2IntMap.BasicEntry<>(slice, rowIndex)));

		Arrays.parallelSort(sortedEntries.elements(), Map.Entry.comparingByKey());

		log.debug("< sorting {}", size);

		return sortedEntries;
	}

	/**
	 * 
	 * @param <T>
	 *            the type of the slice
	 * @param column
	 *            a not-sorted column, where slices are referenced by an index
	 * @param orderedSliceToIndex
	 *            a sorted (by slice) mapping from slice to index
	 * @return
	 */
	@SuppressWarnings("checkstyle:MethodLength")
	protected static <T extends Comparable<T>> IMultitypeColumnFastGet<T> copyToNavigable(
			IMultitypeColumnFastGet<Integer> column,
			Consumer<Object2IntBiConsumer<T>> orderedSliceToIndex) {
		if (column.isEmpty()) {
			return MultitypeNavigableColumn.empty();
		}

		int size = Ints.checkedCast(column.size());
		List<T> slices = new ArrayList<>(size);

		Supplier<LongList> valuesL = Suppliers.memoize(() -> new LongArrayList(size));
		Supplier<DoubleList> valuesD = Suppliers.memoize(() -> new DoubleArrayList(size));
		Supplier<List<Object>> valuesO = Suppliers.memoize(() -> new ArrayList<>(size));

		AtomicReference<Object2LongBiConsumer<T>> refOnLong = new AtomicReference<>();
		AtomicReference<Object2DoubleBiConsumer<T>> refOnDouble = new AtomicReference<>();
		AtomicReference<BiConsumer<T, Object>> refOnObject = new AtomicReference<>();
		AtomicInteger type = new AtomicInteger(IMultitypeConstants.MASK_EMPTY);

		Runnable toObject = () -> {
			if (type.get() == IMultitypeConstants.MASK_LONG) {
				valuesO.get().addAll(valuesL.get());
			} else if (type.get() == IMultitypeConstants.MASK_DOUBLE) {
				valuesO.get().addAll(valuesD.get());
			}

			type.set(IMultitypeConstants.MASK_OBJECT);
			// Further simple add as objects
			refOnLong.set((slice3, v3) -> {
				slices.add(slice3);
				valuesO.get().add(v3);
			});
			refOnDouble.set((slice3, v3) -> {
				slices.add(slice3);
				valuesO.get().add(v3);
			});
			refOnObject.set((slice3, v3) -> {
				slices.add(slice3);
				valuesO.get().add(v3);
			});
		};

		refOnLong.set((slice, v) -> {
			type.set(IMultitypeConstants.MASK_LONG);

			// No need to set type anymore if we stay long
			refOnLong.set((slice2, v2) -> {
				slices.add(slice2);
				valuesL.get().add(v2);
			});
			// If receive double, switch to object
			refOnDouble.set((slice2, v2) -> {
				// Transfer state to object
				toObject.run();

				// Register current entry
				refOnDouble.get().acceptObject2Double(slice2, v2);
			});
			// If receive object, switch to object
			refOnObject.set((slice2, v2) -> {
				// Transfer state to object
				toObject.run();

				// Register current entry
				refOnObject.get().accept(slice2, v2);
			});

			refOnLong.get().acceptObject2Long(slice, v);
		});
		refOnDouble.set((slice, v) -> {
			type.set(IMultitypeConstants.MASK_DOUBLE);

			// No need to set type anymore if we stay double
			refOnDouble.set((slice2, v2) -> {
				slices.add(slice2);
				valuesD.get().add(v2);
			});
			// If receive long, switch to object
			refOnLong.set((slice2, v2) -> {
				// Transfer state to object
				toObject.run();

				// Register current entry
				refOnDouble.get().acceptObject2Double(slice2, v2);
			});
			// If receive object, switch to object
			refOnObject.set((slice2, v2) -> {
				// Transfer state to object
				toObject.run();

				// Register current entry
				refOnObject.get().accept(slice2, v2);
			});

			refOnDouble.get().acceptObject2Double(slice, v);
		});
		refOnObject.set((slice, v) -> {
			toObject.run();

			refOnObject.get().accept(slice, v);
		});

		orderedSliceToIndex.accept((slice, rowIndex) -> {
			column.onValue(rowIndex).acceptReceiver(new IValueReceiver() {
				@Override
				public void onLong(long v) {
					refOnLong.get().acceptObject2Long(slice, v);
				}

				@Override
				public void onDouble(double v) {
					refOnDouble.get().acceptObject2Double(slice, v);
				}

				@Override
				public void onObject(Object v) {
					if (v != null) {
						refOnObject.get().accept(slice, v);
					}
				}
			});
		});

		MultitypeArrayBuilder multitypeArrayBuilder = MultitypeArray.builder();
		if (type.get() == IMultitypeConstants.MASK_EMPTY) {
			return MultitypeNavigableColumn.empty();
		} else if (type.get() == IMultitypeConstants.MASK_LONG) {
			multitypeArrayBuilder.valuesL(valuesL.get()).valuesType(IMultitypeConstants.MASK_LONG);
		} else if (type.get() == IMultitypeConstants.MASK_DOUBLE) {
			multitypeArrayBuilder.valuesD(valuesD.get()).valuesType(IMultitypeConstants.MASK_DOUBLE);
		} else {
			multitypeArrayBuilder.valuesO(valuesO.get()).valuesType(IMultitypeConstants.MASK_OBJECT);
		}

		return MultitypeNavigableColumn.<T>builder()
				.keys(slices)
				.values(multitypeArrayBuilder.build())
				.locked(true)
				.build();
	}

}
