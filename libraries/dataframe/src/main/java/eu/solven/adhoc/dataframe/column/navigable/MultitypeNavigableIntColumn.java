/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.dataframe.column.navigable;

import java.util.AbstractMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.IColumnValueConverter;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.dataframe.IAdhocCapacityConstants;
import eu.solven.adhoc.dataframe.column.IMultitypeArray;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeIntColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeIntColumnFastGetSorted;
import eu.solven.adhoc.dataframe.column.MultitypeArray;
import eu.solven.adhoc.encoding.column.AdhocColumnUnsafe;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.stream.ConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Int-specialized companion to {@link MultitypeNavigableColumn}: stores ({@code int key} -> multitype value) pairs in a
 * primitive {@link IntArrayList} ordered ascending. Complements
 * {@link eu.solven.adhoc.dataframe.column.hash.MultitypeHashIntColumn} (random-access hash variant) and mirrors
 * {@link MultitypeNavigableColumn} (generic variant).
 *
 * <p>
 * Primary use case: the output column of a dictionarizing aggregation layer (e.g. {@code AggregatingColumnsDistinct})
 * where indices are emitted in monotonically-ascending order. The fast {@code onAppendLast} path is therefore the
 * common case, and random insertion is rare. The int-specialized storage avoids the {@code Integer} boxing incurred by
 * {@code MultitypeNavigableColumn<Integer>} on every {@code append} / {@code onValue}.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
@SuppressWarnings({ "PMD.GodClass", "PMD.LooseCoupling" })
public class MultitypeNavigableIntColumn
		implements IMultitypeIntColumnFastGet, IMultitypeIntColumnFastGetSorted, ICompactable, IHasSortedLeg {
	private static final IValueReceiver INSERTION_REJECTED = new IValueReceiver() {
		@Override
		public void onObject(Object v) {
			throw new UnsupportedOperationException("This is a placeholder");
		}
	};

	@Default
	final int capacity = IAdhocCapacityConstants.ZERO_THEN_MAX;

	// This IntArrayList is ordered ascending at all times and contains only distinct elements.
	@Default
	@NonNull
	final IntArrayList keys = new IntArrayList(0);

	@Default
	@NonNull
	final IMultitypeArray values = MultitypeArray.builder().build();

	@Default
	@NonNull
	final IntFunction<IMultitypeArray> valuesGenerator = i -> MultitypeArray.builder().capacity(i).build();

	// Once locked, this can not be written, hence not unlocked.
	@Default
	boolean locked = false;

	// Used to report slowPathes, may be due to bugs/unoptimized_cases/mis-usages.
	final AtomicInteger slowPath = new AtomicInteger();

	// Used to clean lazily a null insertion.
	final AtomicInteger lastInsertionIndex = new AtomicInteger(-1);

	/**
	 * A put operation: it resets the values for given key, initializing it to the provided value.
	 *
	 * @param key
	 *            the int key to append.
	 * @return a {@link IValueReceiver}. If pushing null, this behaves like `.clear`.
	 */
	@Override
	public IValueReceiver append(int key) {
		return write(key, true, true);
	}

	@Override
	public Optional<IValueReceiver> appendIfOptimal(Integer key, boolean distinct) {
		return appendIfOptimal(key.intValue(), distinct);
	}

	/**
	 * Primitive variant of {@link #appendIfOptimal(Integer)} avoiding the boxing that the generic method otherwise
	 * incurs.
	 *
	 * @param key
	 *            the int key to append.
	 * @return a present {@link IValueReceiver} when the key can be appended at the end (optimal path) or merged with an
	 *         existing entry; an empty {@link Optional} when the key would require a random insertion.
	 */
	@Override
	public Optional<IValueReceiver> appendIfOptimal(int key, boolean distinct) {
		IValueReceiver valueReceiver = write(key, true, false);

		if (INSERTION_REJECTED == valueReceiver) {
			return Optional.empty();
		} else {
			return Optional.of(valueReceiver);
		}
	}

	/**
	 * Core write path, mirroring {@link MultitypeNavigableColumn#write}. Handles append-after-last, merge-with-last,
	 * merge-with-existing (via binary search) and random insertion.
	 *
	 * @param key
	 *            the int key to write.
	 * @param mergeElseSet
	 *            when the key already exists, merge (true) or overwrite (false) the previous value.
	 * @param insertEvenIfNotLast
	 *            when the key is new and not greater than the current tail, perform the slow random-insertion path
	 *            (true) or reject with {@link #INSERTION_REJECTED} (false).
	 * @return the {@link IValueReceiver} into which the caller must push the value.
	 */
	protected IValueReceiver write(int key, boolean mergeElseSet, boolean insertEvenIfNotLast) {
		int size = keys.size();
		int valuesSize = values.size();
		if (size != valuesSize) {
			if (keys.isEmpty()) {
				throw new IllegalStateException("keys is empty while values.size() == %s".formatted(values.size()));
			}
			int errorKey = keys.getInt(size - 1);

			if (size > valuesSize) {
				throw new IllegalStateException("Missing push into IValueReceiver for key=%s".formatted(errorKey));
			} else {
				throw new IllegalStateException("Multiple pushes into IValueReceiver for key=%s".formatted(errorKey));
			}
		}

		checkNotLocked(key);

		IValueReceiver valueConsumer;

		boolean keysIsEmpty = keys.isEmpty();
		int comparedWithLast;
		if (keysIsEmpty) {
			comparedWithLast = 0;
		} else {
			comparedWithLast = Integer.compare(key, keys.getInt(keys.size() - 1));
		}
		if (keysIsEmpty || comparedWithLast > 0) {
			valueConsumer = onAppendLast(key);
		} else if (comparedWithLast == 0) {
			int index = keys.size() - 1;
			if (mergeElseSet) {
				valueConsumer = merge(index, key);
			} else {
				valueConsumer = set(index);
			}
		} else {
			int index = getIndex(key);

			if (index >= 0) {
				if (mergeElseSet) {
					valueConsumer = merge(index, key);
				} else {
					valueConsumer = set(index);
				}
			} else if (insertEvenIfNotLast) {
				valueConsumer = onRandomInsertion(key, index);
			} else {
				valueConsumer = INSERTION_REJECTED;
			}
		}
		return valueConsumer;
	}

	/**
	 * Rejects any merge attempt: this column is append-only. A caller reaching this method wrote twice to the same
	 * index, which is illegal for a non-mergeable column.
	 *
	 * @param index
	 *            the index at which the duplicate write was attempted.
	 * @param key
	 *            the duplicated key, purely for diagnostic logging.
	 * @return never: always throws.
	 */
	protected IValueReceiver merge(int index, int key) {
		if (index < 0) {
			throw new ArrayIndexOutOfBoundsException("index=%s must be positive".formatted(index));
		} else if (index >= size()) {
			throw new ArrayIndexOutOfBoundsException("index=%s must be lowerThan size=%s".formatted(index, size()));
		}
		throw new IllegalArgumentException(
				"%s does not allow merging. index=%s key=%s".formatted(getClass(), index, key));
	}

	/**
	 * To be called before a guaranteed `add` operation.
	 */
	protected void checkSizeBeforeAdd() {
		long size = size();

		AdhocColumnUnsafe.checkColumnSize(size);

		if (size == 0) {
			keys.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));

			if (values instanceof MultitypeArray array) {
				array.setCapacity(capacity);
			}
		}
	}

	/**
	 * Given writing is done lazily (by providing a {@link IValueReceiver}), we need to purge after-hand the latest
	 * written value if it is null.
	 */
	// BEWARE This is a very awkward design. This is due to making sure we do not leave any `null` in the column.
	protected void lazyClearLastWrite() {
		if (lastInsertionIndex.get() >= 0) {
			if (values.isNull(lastInsertionIndex.get())) {
				keys.removeInt(lastInsertionIndex.get());
				values.remove(lastInsertionIndex.get());
			}

			lastInsertionIndex.set(-1);
		}
	}

	protected IValueReceiver onAppendLast(int key) {
		checkSizeBeforeAdd();

		lastInsertionIndex.set(keys.size());
		keys.add(key);
		return values.add();
	}

	/**
	 *
	 * @param key
	 *            the out-of-order key being inserted.
	 * @param index
	 *            a negative index representing the insertion index, as returned by {@link IntArrays#binarySearch}.
	 * @return the {@link IValueReceiver} into which the caller must push the value.
	 */
	protected IValueReceiver onRandomInsertion(int key, int index) {
		assert index < 0;

		if (Integer.bitCount(slowPath.incrementAndGet()) == 1) {
			log.warn("Unordered insertion count={} {} < {}", slowPath, key, keys.getInt(keys.size() - 1));
		}
		checkSizeBeforeAdd();

		int insertionIndex = -index - 1;
		lastInsertionIndex.set(insertionIndex);
		keys.add(insertionIndex, key);
		return values.add(insertionIndex);
	}

	protected void doLock() {
		if (!locked) {
			lazyClearLastWrite();

			locked = true;
		}
	}

	protected void checkNotLocked(int key) {
		if (locked) {
			throw new IllegalStateException("This is locked. Can not append key=%s".formatted(key));
		}

		lazyClearLastWrite();
	}

	/**
	 *
	 * @param key
	 *            the int key to search for.
	 * @return the index of the key if present, or a negative value encoding the insertion point (per
	 *         {@link IntArrays#binarySearch} semantics).
	 */
	protected int getIndex(int key) {
		lazyClearLastWrite();

		int size = keys.size();
		if (size == 0) {
			return -1;
		}

		int last = keys.getInt(size - 1);

		if (last == key) {
			return size - 1;
		} else if (last < key) {
			return -size - 1;
		} else {
			return IntArrays.binarySearch(keys.elements(), 0, size, key);
		}
	}

	@Override
	public void scan(IColumnScanner<Integer> rowScanner) {
		doLock();

		int size = keys.size();
		for (int i = 0; i < size; i++) {
			values.read(i).acceptReceiver(rowScanner.onKey(keys.getInt(i)));
		}
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<Integer, U> converter) {
		doLock();

		return IntStream.range(0, Ints.checkedCast(size()))
				.mapToObj(i -> values.apply(i, converter.prepare(keys.getInt(i))));
	}

	@Override
	public IConsumingStream<SliceAndMeasure<Integer>> stream() {
		return IConsumingStream.fromStream(IntStream.range(0, Ints.checkedCast(size()))
				.mapToObj(i -> SliceAndMeasure.<Integer>builder()
						.slice(keys.getInt(i))
						.valueProvider(values.read(i)::acceptReceiver)
						.build()));
	}

	@Override
	public IConsumingStream<SliceAndMeasure<Integer>> limit(int limit) {
		return IConsumingStream.fromStream(IntStream.range(0, Ints.checkedCast(size()))
				.limit(limit)
				.mapToObj(i -> SliceAndMeasure.<Integer>builder()
						.slice(keys.getInt(i))
						.valueProvider(values.read(i)::acceptReceiver)
						.build()));
	}

	@Override
	public IConsumingStream<SliceAndMeasure<Integer>> skip(int skip) {
		return IConsumingStream.fromStream(IntStream.range(0, Ints.checkedCast(size()))
				.skip(skip)
				.mapToObj(i -> SliceAndMeasure.<Integer>builder()
						.slice(keys.getInt(i))
						.valueProvider(values.read(i)::acceptReceiver)
						.build()));
	}

	// CPD-OFF — the switch/case shape and accessor bodies intentionally mirror MultitypeNavigableColumn.
	@SuppressWarnings({ "PMD.ExhaustiveSwitchHasDefault", "CPD-START" })
	@Override
	public IConsumingStream<SliceAndMeasure<Integer>> stream(StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL:
		case StreamStrategy.SORTED_SUB:
			// The whole column is sorted.
			yield stream();
		case StreamStrategy.SORTED_SUB_COMPLEMENT:
			yield IConsumingStream.empty();
		default:
			yield IMultitypeColumnFastGet.defaultStream(this, strategy);
		};
	}

	@Override
	public long size() {
		lazyClearLastWrite();

		return keys.size();
	}

	@Override
	public long size(StreamStrategy strategy) {
		lazyClearLastWrite();

		return switch (strategy) {
		case StreamStrategy.ALL:
		case StreamStrategy.SORTED_SUB:
			yield size();
		case StreamStrategy.SORTED_SUB_COMPLEMENT:
			yield 0;
		};
	}

	@Override
	public boolean isEmpty() {
		lazyClearLastWrite();

		return keys.isEmpty();
	}

	@SuppressWarnings("CPD-END")
	@Override
	public IConsumingStream<Integer> keyStream() {
		doLock();

		return ConsumingStream.<Integer>builder()
				.source(consumer -> keys.intStream().forEach(consumer::accept))
				.build();
	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);

		toStringHelper.add("size", size());

		AtomicInteger index = new AtomicInteger();

		boolean currentLocked = this.locked;
		stream((slice) -> v -> new AbstractMap.SimpleImmutableEntry<>(slice, v))
				.limit(AdhocUnsafe.getLimitOrdinalToString())
				.forEach(sliceToValue -> {
					Integer key = sliceToValue.getKey();
					Object o = sliceToValue.getValue();
					String toStringKey = "#" + index.getAndIncrement() + "-" + key;
					toStringHelper.add(toStringKey, PepperLogHelper.getObjectAndClass(o));
				});
		this.locked = currentLocked;

		return toStringHelper.toString();
	}

	/**
	 * @return an empty and immutable {@link MultitypeNavigableIntColumn}.
	 */
	public static MultitypeNavigableIntColumn empty() {
		return MultitypeNavigableIntColumn.builder()
				.keys(new IntArrayList(0))
				.values(MultitypeArray.empty())
				.locked(true)
				.build();
	}

	@Override
	public MultitypeNavigableIntColumn purgeAggregationCarriers() {
		doLock();

		return MultitypeNavigableIntColumn.builder()
				.keys(keys)
				.locked(true)
				.values(values.purgeAggregationCarriers())
				.build();
	}

	@Override
	public IValueProvider onValue(int key) {
		int index = getIndex(key);

		if (index < 0) {
			return IValueProvider.NULL;
		}
		int hit = index;
		return valueConsumer -> onValue(hit, valueConsumer);
	}

	@Override
	public IValueProvider onValue(Integer key, StreamStrategy strategy) {
		return switch (strategy) {
		// MultitypeNavigableIntColumn IS the sorted leg, so the SORTED_SUB result equals the regular onValue.
		case StreamStrategy.ALL, StreamStrategy.SORTED_SUB -> onValue(key.intValue());
		// The complement (unordered tail) is empty for a fully-sorted navigable column.
		case StreamStrategy.SORTED_SUB_COMPLEMENT -> IValueProvider.NULL;
		};
	}

	protected void onValue(int index, IValueReceiver valueConsumer) {
		values.read(index).acceptReceiver(valueConsumer);
	}

	protected IValueReceiver set(int index) {
		lastInsertionIndex.set(index);
		return values.set(index);
	}

	@Override
	public void compact() {
		lazyClearLastWrite();
		keys.trim();
		if (values instanceof ICompactable compactable) {
			compactable.compact();
		}
	}

	@Override
	public long sortedPrefixLength() {
		return size();
	}
}
