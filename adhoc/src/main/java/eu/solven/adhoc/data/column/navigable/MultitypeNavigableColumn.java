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
package eu.solven.adhoc.data.column.navigable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IAdhocCapacityConstants;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IColumnValueConverter;
import eu.solven.adhoc.data.column.IIsSorted;
import eu.solven.adhoc.data.column.IMultitypeArray;
import eu.solven.adhoc.data.column.IMultitypeColumn;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.MultitypeArray;
import eu.solven.adhoc.data.column.StreamStrategy;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link IMultitypeColumn} relies on {@link List} and {@link Arrays}.
 * <p>
 * The key has to be {@link Comparable}, so that `stream().sorted()` is a no-op, for performance reasons.
 * <p>
 * It can lead to good performance as it is used in context where input slices are inserted in sorted order.
 *
 * @param <T>
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class MultitypeNavigableColumn<T extends Comparable<T>> implements IMultitypeColumnFastGet<T>, IIsSorted {
	private static final IValueReceiver INSERTION_REJECTED = new IValueReceiver() {

		@Override
		public void onObject(Object v) {
			throw new UnsupportedOperationException("This is a placeholder");
		}
	};

	@Default
	final int capacity = IAdhocCapacityConstants.ZERO_THEN_MAX;

	// This List is ordered at all times
	// This List has only distinct elements
	@Default
	@NonNull
	final List<T> keys = new ObjectArrayList<>(0);

	@Default
	@NonNull
	final IMultitypeArray values = MultitypeArray.builder().build();

	@Default
	@NonNull
	final IntFunction<IMultitypeArray> valuesGenerator = i -> MultitypeArray.builder().capacity(i).build();

	// Once locked, this can not be written, hence not unlocked
	@Default
	boolean locked = false;

	// Used to report slowPathes, may be due to bugs/unoptimized_cases/mis-usages
	final AtomicInteger slowPath = new AtomicInteger();

	// Used to clean lazily a null insertion
	final AtomicInteger lastInsertionIndex = new AtomicInteger(-1);

	protected IValueReceiver merge(int index) {
		if (index < 0) {
			throw new ArrayIndexOutOfBoundsException("index=%s must be positive".formatted(index));
		} else if (index >= size()) {
			throw new ArrayIndexOutOfBoundsException("index=%s must be lowerThan size=%s".formatted(index, size()));
		}
		throw new IllegalArgumentException(
				"%s does not allow merging. index=%s key=%s".formatted(getClass(), index, keys.get(index)));
	}

	@Override
	public IValueReceiver set(T key) {
		return write(key, false, true);
	}

	/**
	 * A put operation: it resets the values for given key, initializing it to the provided value.
	 *
	 * @param key
	 * @return a {@link IValueReceiver}. If pushing null, this behave like `.clear`
	 */
	@Override
	public IValueReceiver append(T key) {
		return write(key, true, true);
	}

	public Optional<IValueReceiver> appendIfOptimal(T key) {
		IValueReceiver valueReceiver = write(key, true, false);

		if (INSERTION_REJECTED == valueReceiver) {
			return Optional.empty();
		} else {
			return Optional.of(valueReceiver);
		}
	}

	/**
	 * To be called before a guaranteed `add` operation.
	 */
	@SuppressWarnings("PMD.LooseCoupling")
	protected void checkSizeBeforeAdd() {
		long size = size();
		if (size >= AdhocUnsafe.limitColumnSize) {
			// TODO Log the first and last elements
			throw new IllegalStateException(
					"Can not add as size=%s and limit=%s".formatted(size, AdhocUnsafe.limitColumnSize));
		} else if (size == 0) {
			if (keys instanceof ArrayList<?> arrayList) {
				arrayList.ensureCapacity(capacity);
			} else if (keys instanceof ObjectArrayList<?> arrayList) {
				arrayList.ensureCapacity(capacity);
			}
		}
	}

	/**
	 * 
	 * @param key
	 * @param mergeElseSet
	 *            if true and the key has already a value, we merge the input with the current value. Else we replace
	 *            the existing value by the provided value.
	 * @param insertEvenIfNotLast
	 *            if true, and given key is new but not last, we do an random insertion (which is slow as it requires
	 *            shitfing following keys and values). If false, returns a null IValueReceiver.
	 * @return
	 */
	protected IValueReceiver write(T key, boolean mergeElseSet, boolean insertEvenIfNotLast) {
		int size = keys.size();
		int valuesSize = values.size();
		if (size != valuesSize) {
			if (keys.isEmpty()) {
				throw new IllegalStateException("keys is empty while values.size() == %s".formatted(values.size()));
			}
			// This is generally the faulty key
			T errorKey = keys.getLast();

			if (size > valuesSize) {
				// BEWARE Should we prefer trimming the unwritten keys?
				throw new IllegalStateException("Missing push into IValueReceiver for key=%s".formatted(errorKey));
			} else if (size < valuesSize) {
				throw new IllegalStateException("Multiple pushes into IValueReceiver for key=%s".formatted(errorKey));
			}
		}

		checkNotLocked(key);

		IValueReceiver valueConsumer;

		if (keys.isEmpty() || key.compareTo(keys.getLast()) > 0) {
			checkSizeBeforeAdd();

			// In most cases, we append a greater key, because we process sorted keys
			lastInsertionIndex.set(keys.size());
			keys.add(key);
			valueConsumer = values.add();
		} else {
			int index = getIndex(key);

			if (index >= 0) {
				if (mergeElseSet) {
					valueConsumer = merge(index);
				} else {
					valueConsumer = set(index);
				}
			} else {
				if (insertEvenIfNotLast) {
					checkSizeBeforeAdd();
					valueConsumer = onRandomInsertion(key, index);
				} else {
					valueConsumer = INSERTION_REJECTED;
				}
			}
		}
		return valueConsumer;
	}

	// BEWARE This is a very awkward design. This is due to making sure we do not leave any `null` in the column.
	protected void lazyClearLastWrite() {
		if (lastInsertionIndex.get() >= 0 && IValueProvider.isNull(values.read(lastInsertionIndex.get()))) {
			keys.remove(lastInsertionIndex.get());
			values.remove(lastInsertionIndex.get());

			lastInsertionIndex.set(-1);
		}
	}

	protected IValueReceiver onRandomInsertion(T key, int index) {
		// BEWARE This case should be rare. For now, we try handling it smoothly
		// It typically happens if it is used to receive table aggregates while the table does not provide
		// sorted results.
		// It typically happens if some underlying queryStep does not return a properly sorted column.
		if (Integer.bitCount(slowPath.incrementAndGet()) == 1) {
			log.warn("Unordered insertion count={} {} < {}", slowPath, key, keys.getLast());
		}
		int insertionIndex = -index - 1;
		lastInsertionIndex.set(insertionIndex);
		keys.add(insertionIndex, key);
		return values.add(insertionIndex);
	}

	protected void doLock() {
		if (!locked) {
			lazyClearLastWrite();

			locked = true;
			assert keys.stream().distinct().count() == keys.size() : "multiple .put with same key is illegal";
		}
	}

	protected void checkNotLocked(T key) {
		if (locked) {
			throw new IllegalStateException("This is locked. Can not append key=%s".formatted(key));
		}

		lazyClearLastWrite();
	}

	protected int getIndex(T key) {
		lazyClearLastWrite();

		if (keys.isEmpty()) {
			return -1;
		}
		int compareWithLast = keys.getLast().compareTo(key);

		if (compareWithLast == 0) {
			// In a Bucketor, we often write into the previously written slice
			// e.g. `a=a1&b=b1` and `a=a1&b=b2` would both write into `a=a1`

			// Merge with last element
			return keys.size() - 1;
		} else if (compareWithLast < 0) {
			// Append after last
			return -keys.size();
		} else {
			// slow path: merge with an existing element
			return Collections.binarySearch(keys, key, Comparator.naturalOrder());
		}
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		doLock();

		int size = keys.size();
		for (int i = 0; i < size; i++) {
			values.read(i).acceptReceiver(rowScanner.onKey(keys.get(i)));
		}
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		doLock();

		return IntStream.range(0, Ints.checkedCast(size()))
				.mapToObj(i -> values.apply(i, converter.prepare(keys.get(i))));
	}

	@Override
	public Stream<SliceAndMeasure<T>> stream() {
		return IntStream.range(0, Ints.checkedCast(size()))
				.mapToObj(i -> SliceAndMeasure.<T>builder()
						.slice(keys.get(i))
						.valueProvider(vc -> values.read(i).acceptReceiver(vc))
						.build());
	}

	@SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
	@Override
	public Stream<SliceAndMeasure<T>> stream(StreamStrategy stragegy) {
		return switch (stragegy) {
		case StreamStrategy.ALL:
		case StreamStrategy.SORTED_SUB:
			// The whole column is sorted
			yield stream();
		case StreamStrategy.SORTED_SUB_COMPLEMENT:
			yield Stream.empty();
		default:
			yield IMultitypeColumn.defaultStream(this, stragegy);
		};
	}

	@Override
	public long size() {
		lazyClearLastWrite();

		return keys.size();
	}

	@Override
	public boolean isEmpty() {
		lazyClearLastWrite();

		return keys.isEmpty();
	}

	@Override
	public Stream<T> keyStream() {
		doLock();

		// No need for .distinct as each key is guaranteed to appear in a single column
		return StreamSupport.stream(Spliterators.spliterator(keys, // keys is guaranteed to hold distinct value
				Spliterator.DISTINCT
						// keys are sorted naturally
						| Spliterator.ORDERED
						| Spliterator.SORTED
						// When read, this can not be edited anymore
						| Spliterator.IMMUTABLE),
				false);

	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);

		toStringHelper.add("size", size());

		AtomicInteger index = new AtomicInteger();

		stream((slice) -> v -> new AbstractMap.SimpleImmutableEntry<>(slice, v)).limit(AdhocUnsafe.limitOrdinalToString)
				.forEach(sliceToValue -> {
					T k = sliceToValue.getKey();
					Object o = sliceToValue.getValue();
					toStringHelper.add("#" + index.getAndIncrement(), k + "->" + PepperLogHelper.getObjectAndClass(o));
				});

		return toStringHelper.toString();
	}

	/**
	 * @return an empty and immutable MultiTypeStorage
	 */
	public static <T extends Comparable<T>> MultitypeNavigableColumn<T> empty() {
		return MultitypeNavigableColumn.<T>builder()
				.keys(Collections.emptyList())
				.values(MultitypeArray.empty())
				.build();
	}

	@Override
	public MultitypeNavigableColumn<T> purgeAggregationCarriers() {
		doLock();

		return MultitypeNavigableColumn.builder()
				.keys((List) keys)
				.locked(true)
				.values(values.purgeAggregationCarriers())
				.build();
	}

	@Override
	public IValueProvider onValue(T key) {
		int index = getIndex(key);

		if (index < 0) {
			return IValueProvider.NULL;
		} else {
			return valueConsumer -> onValue(index, valueConsumer);
		}
	}

	protected void onValue(int index, IValueReceiver valueConsumer) {
		values.read(index).acceptReceiver(valueConsumer);
	}

	protected IValueReceiver set(int index) {
		lastInsertionIndex.set(index);
		return values.set(index);
	}

	@Deprecated(since = "For unitTest purposes")
	public void clearKey(T key) {
		int index = getIndex(key);
		if (index >= 0) {
			keys.remove(index);
			values.remove(index);

			if (index == lastInsertionIndex.get()) {
				lastInsertionIndex.set(-1);
			}
		}
	}

	// @Override
	// public void ensureCapacity(int capacity) {
	// if (keys instanceof ArrayList<?> arrayList) {
	// arrayList.ensureCapacity(capacity);
	// } else if (keys instanceof ObjectArrayList<?> arrayList) {
	// arrayList.ensureCapacity(capacity);
	// }
	// }
}
