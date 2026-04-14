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
package eu.solven.adhoc.dataframe.column.navigable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import eu.solven.adhoc.dataframe.collection.ChunkedList;
import eu.solven.adhoc.dataframe.column.IMultitypeArray;
import eu.solven.adhoc.dataframe.column.IMultitypeColumn;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGetSorted;
import eu.solven.adhoc.dataframe.column.MultitypeArray;
import eu.solven.adhoc.dataframe.column.hash.CleaningValueReceiver;
import eu.solven.adhoc.encoding.column.AdhocColumnUnsafe;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.stream.ConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;
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
public class MultitypeNavigableColumn<T extends Comparable<T>>
		implements IMultitypeColumnFastGetSorted<T>, ICompactable, IHasSortedLeg {
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
	final List<T> keys = new ChunkedList<>();

	@Default
	@NonNull
	// cleanIfDirty is false as we need to write the null, to later remove it with the key
	// BEWARE The design is wrong.
	final IMultitypeArray values = MultitypeArray.builder().cleanDirty(false).build();

	// If true, this will automatically turn dirty input (like `Integer`) into a clean one (like `int`)
	@Default
	boolean cleanDirty = CleaningValueReceiver.DEFAULT;

	// Once locked, this can not be written, hence not unlocked
	@Default
	boolean locked = false;

	// Used to report slowPathes, may be due to bugs/unoptimized_cases/mis-usages
	final AtomicInteger slowPath = new AtomicInteger();

	// Used to clean lazily a null insertion
	final AtomicInteger lastInsertionIndex = new AtomicInteger(-1);

	/**
	 * Factory for the lazily-built {@link IKeyPresencePreScreen}, called once with the column's {@code capacity}.
	 * Default is {@link BloomKeyPresencePreScreenFactory#INSTANCE} which produces a Bloom-backed pre-screen at the
	 * default tuning.
	 *
	 * <p>
	 * Override via the builder to plug in a different implementation, e.g.
	 * {@link NoopKeyPresencePreScreenFactory#INSTANCE} to disable the optimization (legacy behavior of always falling
	 * through to the exact binary search), or a domain-specific factory tuned for the actual key type.
	 */
	@Default
	@NonNull
	final IKeyPresencePreScreenFactory presenceFilterFactory = BloomKeyPresencePreScreenFactory.INSTANCE;

	/**
	 * Lazily initialized {@link IKeyPresencePreScreen} used by {@link #appendIfOptimal} as a fast pre-check before the
	 * binary-search slow path: if the pre-screen returns {@code mightContain == false}, the key is definitely not
	 * present and we can reject the insertion immediately without paying for the {@link Collections#binarySearch} call.
	 *
	 * <p>
	 * The reference holder is {@code final} so this field is excluded from the {@link SuperBuilder} (it is not a
	 * caller-tunable knob — pass a {@link #presenceFilterFactory} to customise the implementation); the wrapped
	 * pre-screen is created on first call to {@link #appendIfOptimal} and updated by every subsequent insertion via
	 * {@link #onAppendLast} and {@link #onRandomInsertion}.
	 *
	 * <p>
	 * <strong>BEWARE</strong> the pre-screen is <em>never</em> cleared or rebuilt by {@link #lazyClearLastWrite()} (nor
	 * by {@link #clearKey(Comparable)}). {@link #lazyClearLastWrite} purges a key from {@code keys}/{@code values} but
	 * {@link IKeyPresencePreScreen} implementations are not required to support removal (and the default Bloom
	 * implementation by definition cannot). This is harmless because the pre-screen is only a fast-reject filter: a
	 * false positive ({@code mightContain == true} for a key that was actually purged) just falls through to the exact
	 * {@link #getIndex} binary search, which then either finds the key or rejects the append. The pre-screen never
	 * excludes a real entry, so correctness is preserved; the only downside is a slightly lower hit rate on the
	 * optimization after many cleared writes.
	 */
	final AtomicReference<IKeyPresencePreScreen<T>> presenceFilter = new AtomicReference<>();

	protected IValueReceiver merge(int index) {
		if (index < 0) {
			throw new ArrayIndexOutOfBoundsException("index=%s must be positive".formatted(index));
		} else if (index >= size()) {
			throw new ArrayIndexOutOfBoundsException("index=%s must be lowerThan size=%s".formatted(index, size()));
		}
		throw new IllegalArgumentException(
				"%s does not allow merging. index=%s key=%s".formatted(getClass(), index, keys.get(index)));
	}

	/**
	 * A put operation: it resets the values for given key, initializing it to the provided value.
	 *
	 * @param key
	 * @return a {@link IValueReceiver}. If pushing null, this behaves like `.clear`
	 */
	@Override
	public IValueReceiver append(T key) {
		return write(key, true, true);
	}

	@Override
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

		// TODO Log the first and last elements
		AdhocColumnUnsafe.checkColumnSize(size);

		if (size == 0) {
			if (keys instanceof ArrayList<?> arrayList) {
				arrayList.ensureCapacity(capacity);
			} else if (keys instanceof ObjectArrayList<?> arrayList) {
				arrayList.ensureCapacity(capacity);
			}

			if (values instanceof MultitypeArray array) {
				array.setCapacity(capacity);
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

		boolean keysIsEmpty = keys.isEmpty();
		int comparedWithLast;
		if (keysIsEmpty) {
			comparedWithLast = 0;
		} else {
			comparedWithLast = key.compareTo(keys.getLast());
		}
		if (keysIsEmpty || comparedWithLast > 0) {
			valueConsumer = onAppendLast(key);
		} else if (comparedWithLast == 0) {
			// In many cases, we accumulate in the greater/latest key, because we induce by removing columns
			int index = keys.size() - 1;
			if (mergeElseSet) {
				valueConsumer = merge(index);
			} else {
				valueConsumer = set(index);
			}
		} else {
			int index = getIndex(key, insertEvenIfNotLast);

			if (index >= 0) {
				if (mergeElseSet) {
					valueConsumer = merge(index);
				} else {
					valueConsumer = set(index);
				}
			} else {
				if (insertEvenIfNotLast) {
					valueConsumer = onRandomInsertion(key, index);
				} else {
					// Pre-screen pre-check for the appendIfOptimal path: if the pre-screen says definitely-not-present,
					// skip the binary-search slow path and reject immediately. The pre-screen never excludes a real
					// entry,
					// so a positive result still falls through to the exact `getIndex` lookup below.
					valueConsumer = INSERTION_REJECTED;
				}
			}
		}
		if (!cleanDirty || valueConsumer == INSERTION_REJECTED) {
			return valueConsumer;
		} else {
			// BEWARE Must not clean nulls, as we need to detect after hand a null to also clear the key
			return CleaningValueReceiver.cleaning(cleanDirty, false, new IValueReceiver() {

				@Override
				public void onLong(long v) {
					valueConsumer.onLong(v);
				}

				@Override
				public void onDouble(double v) {
					valueConsumer.onDouble(v);
				}

				@Override
				public void onObject(Object v) {
					valueConsumer.onObject(v);
				}
			});
		}
	}

	/**
	 * Given writing is done lazily (by providing a {@link IValueReceiver}), we need to purge after-hand the latest
	 * written value if it is null.
	 */
	// BEWARE This is a very awkward design. This is due to making sure we do not leave any `null` in the column.
	// BEWARE This does NOT clear the entry from `presenceFilter` — IKeyPresencePreScreen implementations are not
	// required to support removal (and the default Bloom-backed impl by definition cannot). The purged key remains
	// in the pre-screen and may produce a `mightContain == true` false positive on a subsequent `appendIfOptimal`,
	// which then falls through to the exact `getIndex` lookup. See `presenceFilter` field Javadoc.
	protected void lazyClearLastWrite() {
		if (lastInsertionIndex.get() >= 0) {
			if (values.isNull(lastInsertionIndex.get())) {
				keys.remove(lastInsertionIndex.get());
				values.remove(lastInsertionIndex.get());
			}

			lastInsertionIndex.set(-1);
		}
	}

	protected IValueReceiver onAppendLast(T key) {
		checkSizeBeforeAdd();

		// In most cases, we append a greater key, because we process sorted keys
		lastInsertionIndex.set(keys.size());
		keys.add(key);
		recordPresenceKey(key);
		return values.add();
	}

	/**
	 * Records {@code key} in the {@link #presenceFilter} if it has been initialized. No-op when the pre-screen has
	 * never been queried (no {@link #appendIfOptimal} call has occurred yet) — in that case it will be lazily built
	 * from the current {@code keys} list on first access.
	 */
	protected void recordPresenceKey(T key) {
		IKeyPresencePreScreen<T> pf = presenceFilter.get();
		if (pf != null) {
			pf.add(key);
		}
	}

	/**
	 * Returns the lazily-initialized {@link #presenceFilter}, building it from the current {@code keys} list on first
	 * access via the {@link #presenceFilterFactory}.
	 */
	protected IKeyPresencePreScreen<T> getOrCreatePresenceFilter() {
		IKeyPresencePreScreen<T> pf = presenceFilter.get();
		if (pf == null) {
			pf = presenceFilterFactory.create(capacity);
			// Seed with any keys already present (the column may have been populated before the first
			// appendIfOptimal call).
			for (T existing : keys) {
				pf.add(existing);
			}
			presenceFilter.set(pf);
		}
		return pf;
	}

	/**
	 *
	 * @param key
	 * @param index
	 *            a negative index representing the insertion index.
	 * @return
	 */
	protected IValueReceiver onRandomInsertion(T key, int index) {
		assert index < 0;

		// BEWARE This case should be rare. For now, we try handling it smoothly
		// It typically happens if it is used to receive table aggregates while the table does not provide
		// sorted results.
		// It typically happens if some underlying queryStep does not return a properly sorted column.
		if (Integer.bitCount(slowPath.incrementAndGet()) == 1) {
			log.warn("Unordered insertion count={} {} < {}", slowPath, key, keys.getLast());
		}
		checkSizeBeforeAdd();

		int insertionIndex = -index - 1;
		lastInsertionIndex.set(insertionIndex);
		keys.add(insertionIndex, key);
		recordPresenceKey(key);
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

	/**
	 *
	 * @param key
	 * @return the index of the key, or either `insertEvenIfNotLast==true` and return insertionIndex, else any negative
	 *         value.
	 */
	protected int getIndex(T key, boolean insertEvenIfNotLast) {
		lazyClearLastWrite();

		if (keys.isEmpty()) {
			return -1;
		}

		// BEWARE Can not rely on presenceFilter as, in case of absence, we return the insertionIndex
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
			if (!insertEvenIfNotLast && !getOrCreatePresenceFilter().mightContain(key)) {
				// Not interested in insertionIndex, and we know the key is not present
				return -1;
			} else {
				return Collections.binarySearch(keys, key, Comparator.naturalOrder());
			}
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
	public IConsumingStream<SliceAndMeasure<T>> stream() {
		return IConsumingStream.fromStream(IntStream.range(0, Ints.checkedCast(size()))
				.mapToObj(i -> SliceAndMeasure.<T>builder()
						.slice(keys.get(i))
						.valueProvider(values.read(i)::acceptReceiver)
						.build()));
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> limit(int limit) {
		return IConsumingStream.fromStream(IntStream.range(0, Ints.checkedCast(size()))
				.limit(limit)
				.mapToObj(i -> SliceAndMeasure.<T>builder()
						.slice(keys.get(i))
						.valueProvider(values.read(i)::acceptReceiver)
						.build()));
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> skip(int skip) {
		return IConsumingStream.fromStream(IntStream.range(0, Ints.checkedCast(size()))
				.skip(skip)
				.mapToObj(i -> SliceAndMeasure.<T>builder()
						.slice(keys.get(i))
						.valueProvider(values.read(i)::acceptReceiver)
						.build()));
	}

	@SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
	@Override
	public IConsumingStream<SliceAndMeasure<T>> stream(StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL:
		case StreamStrategy.SORTED_SUB:
			// The whole column is sorted
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
			// The whole column is sorted
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

	@Override
	public IConsumingStream<T> keyStream() {
		doLock();

		return ConsumingStream.<T>builder().source(keys::forEach).build();
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
					T k = sliceToValue.getKey();
					Object o = sliceToValue.getValue();
					toStringHelper.add("#" + index.getAndIncrement(), k + "->" + PepperLogHelper.getObjectAndClass(o));
				});
		// Restore the locked status so that `.toString` in debug does not lock the instance
		this.locked = currentLocked;

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
	public IMultitypeColumnFastGetSorted<T> purgeAggregationCarriers() {
		doLock();

		return MultitypeNavigableColumn.<T>builder()
				.keys(keys)
				.locked(true)
				.values(values.purgeAggregationCarriers())
				.build();
	}

	@Override
	public IValueProvider onValue(T key) {
		int index = getIndex(key, false);

		if (index < 0) {
			return IValueProvider.NULL;
		} else {
			return valueConsumer -> onValue(index, valueConsumer);
		}
	}

	@Override
	public IValueProvider onValue(T key, StreamStrategy strategy) {
		return switch (strategy) {
		// MultitypeNavigableColumn IS the sorted leg, so the SORTED_SUB result equals the regular onValue.
		case StreamStrategy.ALL, StreamStrategy.SORTED_SUB -> onValue(key);
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

	@Deprecated(since = "For unitTest purposes")
	public void clearKey(T key) {
		int index = getIndex(key, false);
		if (index >= 0) {
			keys.remove(index);
			values.remove(index);

			if (index == lastInsertionIndex.get()) {
				lastInsertionIndex.set(-1);
			}
		}
	}

	@Override
	public void compact() {
		lazyClearLastWrite();
		if (values instanceof ICompactable compactable) {
			compactable.compact();
		}
	}

	@Override
	public long sortedPrefixLength() {
		return size();
	}
}
