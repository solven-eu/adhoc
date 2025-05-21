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
package eu.solven.adhoc.data.column;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.MultitypeArray.MultitypeArrayBuilder;
import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleImmutableList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongImmutableList;
import it.unimi.dsi.fastutil.objects.AbstractObject2DoubleMap;
import it.unimi.dsi.fastutil.objects.AbstractObject2LongMap;
import it.unimi.dsi.fastutil.objects.AbstractObject2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link IMultitypeColumn} relies on {@link List} and {@link Arrays}.
 * <p>
 * The key has to be {@link Comparable}, so that `stream().sorted()` is a no-op, for performance reasons.
 *
 * @param <T>
 */
@SuperBuilder
@Slf4j
public class MultitypeNavigableColumn<T extends Comparable<T>> implements IMultitypeColumnFastGet<T> {

	// This List is ordered at all times
	// This List has only distinct elements
	@Default
	@NonNull
	// TODO Capacity strategy?
	final List<T> keys = new ArrayList<>();

	@Default
	@NonNull
	final IMultitypeArray values = MultitypeArray.builder().build();

	// Once locked, this can not be written, hence not unlocked
	@Default
	boolean locked = false;

	// Used to report slowPathes, may be due to bugs/unoptimized_cases/mis-usages
	final AtomicInteger slowPath = new AtomicInteger();

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
		return write(key, false);
	}

	/**
	 * A put operation: it resets the values for given key, initializing it to the provided value.
	 *
	 * @param key
	 * @return a {@link IValueReceiver}. If pushing null, this behave like `.clear`
	 */
	@Override
	public IValueReceiver append(T key) {
		return write(key, true);
	}

	protected IValueReceiver write(T key, boolean mergeElseSet) {
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
			// In most cases, we append a greater key, because we process sorted keys
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
				// BEWARE This case should not happen. For now, we try handling it smoothly
				// It typically happens if it is used to receive table aggregates while the table does not provide
				// sorted results.
				// It typically happens if some underlying queryStep does not return a properly sorted column.
				if (Integer.bitCount(slowPath.incrementAndGet()) == 1) {
					log.warn("Unordered insertion count={} {} < {}", slowPath, key, keys.getLast());
				}
				int insertionIndex = -index - 1;
				keys.add(insertionIndex, key);
				valueConsumer = values.add(insertionIndex);
			}
		}
		return valueConsumer;
	}

	protected void doLock() {
		if (!locked) {
			locked = true;
			assert keys.stream().distinct().count() == keys.size() : "multiple .put with same key is illegal";
		}
	}

	protected void checkNotLocked(T key) {
		if (locked) {
			throw new IllegalStateException("This is locked. Can not append key=%s".formatted(key));
		}
	}

	protected int getIndex(T key) {
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
		doLock();

		return IntStream.range(0, Ints.checkedCast(size()))
				.mapToObj(i -> SliceAndMeasure.<T>builder()
						.slice(keys.get(i))
						.valueProvider(vc -> values.read(i).acceptReceiver(vc))
						.build());
	}

	@Override
	public long size() {
		return keys.size();
	}

	@Override
	public boolean isEmpty() {
		return keys.isEmpty();
	}

	@Override
	public Stream<T> keyStream() {
		doLock();

		// No need for .distinct as each key is guaranteed to appear in a single column
		return StreamSupport.stream(Spliterators.spliterator(keys, // keys is guaranteed to hold distinct value
				Spliterator.DISTINCT |
				// keys are sorted naturally
						Spliterator.ORDERED | Spliterator.SORTED |
						// When read, this can not be edited anymore
						Spliterator.IMMUTABLE),
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
	public void purgeAggregationCarriers() {
		values.replaceAllObjects(value -> {
			if (value instanceof IAggregationCarrier aggregationCarrier) {
				return IValueProvider.getValue(vc -> aggregationCarrier.acceptValueReceiver(vc));
			} else {
				return value;
			}
		});
	}

	@Override
	public IValueProvider onValue(T key) {
		int index = getIndex(key);

		if (index < 0) {
			return valueConsumer -> valueConsumer.onObject(null);
		} else {
			return valueConsumer -> onValue(index, valueConsumer);
		}
	}

	protected void onValue(int index, IValueReceiver valueConsumer) {
		values.read(index).acceptReceiver(valueConsumer);
	}

	protected IValueReceiver set(int index) {
		return values.set(index);
	}

	/**
	 * 
	 * @param <T>
	 * @param input
	 * @return a copy into a {@link MultitypeNavigableColumn}
	 */
	public static <T extends Comparable<T>> IMultitypeColumnFastGet<T> copy(IMultitypeColumnFastGet<T> input) {
		int size = Ints.checkedCast(input.size());

		if (size == 0) {
			return empty();
		}

		AtomicLong nbLong = new AtomicLong();
		AtomicLong nbDouble = new AtomicLong();
		ObjectList<Map.Entry<T, ?>> keyToObject = new ObjectArrayList<>(size);

		input.stream().forEach(sm -> {
			sm.getValueProvider().acceptReceiver(new IValueReceiver() {

				@Override
				public void onLong(long v) {
					nbLong.incrementAndGet();
					keyToObject.add(new AbstractObject2LongMap.BasicEntry<T>(sm.getSlice(), v));
				}

				@Override
				public void onDouble(double v) {
					nbDouble.incrementAndGet();
					keyToObject.add(new AbstractObject2DoubleMap.BasicEntry<T>(sm.getSlice(), v));
				}

				@Override
				public void onObject(Object v) {
					keyToObject.add(new AbstractObject2ObjectMap.BasicEntry<T, Object>(sm.getSlice(), v));
				}
			});
		});

		MultitypeArrayBuilder multitypeArrayBuilder = MultitypeArray.builder();

		final ImmutableList.Builder<T> keys = ImmutableList.builderWithExpectedSize(size);

		// https://stackoverflow.com/questions/17328077/difference-between-arrays-sort-and-arrays-parallelsort
		// This section typically takes from 100ms to 1s for 100k slices
		keyToObject.sort((Comparator) Map.Entry.<T, Object>comparingByKey());

		if (nbLong.get() == size) {
			final LongArrayList values = new LongArrayList(size);

			keyToObject.forEach(e -> {
				keys.add(e.getKey());
				values.add(((Object2LongMap.Entry<?>) e).getLongValue());
			});

			multitypeArrayBuilder.valuesL(LongImmutableList.of(values.elements()))
					.valuesType(IMultitypeConstants.MASK_LONG);
		} else if (nbDouble.get() == size) {
			final DoubleArrayList values = new DoubleArrayList(size);

			keyToObject.forEach(e -> {
				keys.add(e.getKey());
				values.add(((Object2DoubleMap.Entry<?>) e).getDoubleValue());
			});

			multitypeArrayBuilder.valuesD(DoubleImmutableList.of(values.elements()))
					.valuesType(IMultitypeConstants.MASK_DOUBLE);
		} else {
			final ImmutableList.Builder<Object> values = ImmutableList.builderWithExpectedSize(size);

			keyToObject.forEach(e -> {
				keys.add(e.getKey());
				values.add(e.getValue());
			});

			multitypeArrayBuilder.valuesO(values.build()).valuesType(IMultitypeConstants.MASK_OBJECT);
		}

		return MultitypeNavigableColumn.<T>builder()
				.keys(keys.build())
				.values(multitypeArrayBuilder.build())
				.locked(true)
				.build();
	}

	public void clearKey(T key) {
		int index = getIndex(key);
		if (index >= 0) {
			keys.remove(index);
			values.remove(index);
		}
	}
}
