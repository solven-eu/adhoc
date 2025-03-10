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
package eu.solven.adhoc.storage.column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.storage.IValueConsumer;
import eu.solven.adhoc.storage.IValueProvider;
import eu.solven.adhoc.storage.column.MultitypeArray.MultitypeArrayBuilder;
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
	final MultitypeArray values = MultitypeArray.builder().build();

	// Once locked, this can not be written, hence not unlocked
	@Default
	boolean locked = false;

	final AtomicInteger slowPath = new AtomicInteger();

	protected IValueConsumer merge(int index) {
		throw new IllegalArgumentException("This does not allow merging. key=%s".formatted(keys.get(index)));
	}

	/**
	 * A put operation: it resets the values for given key, initializing it to the provided value.
	 *
	 * @param key
	 * @return a {@link IValueConsumer}. If pushing null, this behave like `.clear`
	 */
	@Override
	public IValueConsumer append(T key) {
		checkLock(key);
		
		IValueConsumer valueConsumer;
		
		if (keys.isEmpty() || key.compareTo(keys.getLast()) > 0) {
			// In most cases, we append a greater key, because we process sorted keys
			keys.add(key);
			valueConsumer = values.add();
		} else {
			int index = getIndex(key);
			
			if (index >= 0) {
				valueConsumer = merge(index);
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

	protected void checkLock(T key) {
		if (locked) {
			throw new IllegalStateException("This is locked. Can not append %s".formatted(key));
		}
	}

	protected int getIndex(T key) {
		return Collections.binarySearch(keys, key, Comparator.naturalOrder());
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		doLock();

		int size = keys.size();
		for (int i = 0; i < size; i++) {
			values.read(i, rowScanner.onKey(keys.get(i)));
		}
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		doLock();

		return LongStream.range(0, size())
				.mapToInt(Ints::checkedCast)
				.mapToObj(i -> values.apply(i, converter.prepare(keys.get(i))));
	}

	@Override
	public Stream<SliceAndMeasure<T>> stream() {
		doLock();

		return LongStream.range(0, size())
				.mapToInt(Ints::checkedCast)
				.mapToObj(i -> SliceAndMeasure.<T>builder()
						.slice(keys.get(i))
						.valueProvider(vc -> values.read(i, vc))
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

		AtomicInteger index = new AtomicInteger();

		stream((slice) -> v -> Map.of("k", slice, "v", v)).limit(AdhocUnsafe.limitOrdinalToString)
				.forEach(sliceToValue -> {
					Object k = sliceToValue.get("k");
					Object o = sliceToValue.get("v");
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
		values.replaceObjects(value -> {
			if (value instanceof IAggregationCarrier aggregationCarrier) {
				return IValueProvider.getValue(vc -> aggregationCarrier.acceptValueConsumer(vc));
			} else {
				return value;
			}
		});
	}

	@Override
	public void onValue(T key, IValueConsumer valueConsumer) {
		int index = getIndex(key);

		if (index < 0) {
			valueConsumer.onObject(null);
		} else {
			onValue(index, valueConsumer);
		}
	}

	protected void onValue(int index, IValueConsumer valueConsumer) {
		values.read(index, valueConsumer);
	}

	public static <T extends Comparable<T>> IMultitypeColumnFastGet<T> copy(IMultitypeColumnFastGet<T> input) {
		int size = Ints.checkedCast(input.size());

		if (size == 0) {
			return empty();
		}

		ObjectList<Object2LongMap.Entry<T>> keyToLong = new ObjectArrayList<>(size);
		ObjectList<Object2DoubleMap.Entry<T>> keyToDouble = new ObjectArrayList<>(size);
		ObjectList<Map.Entry<T, ?>> keyToObject = new ObjectArrayList<>(size);

		input.stream().forEach(sm -> {
			sm.getValueProvider().acceptConsumer(new IValueConsumer() {

				@Override
				public void onLong(long v) {
					keyToLong.add(new AbstractObject2LongMap.BasicEntry<T>(sm.getSlice(), v));
				}

				@Override
				public void onDouble(double v) {
					keyToDouble.add(new AbstractObject2DoubleMap.BasicEntry<T>(sm.getSlice(), v));
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
		if (keyToLong.size() == size) {
			keyToLong.sort(Map.Entry.comparingByKey());

			final LongArrayList values = new LongArrayList(size);

			keyToLong.forEach(e -> {
				keys.add(e.getKey());
				values.add(e.getLongValue());
			});

			multitypeArrayBuilder.valuesL(LongImmutableList.of(values.elements())).valuesType((byte) 1);
		} else if (keyToDouble.size() == size) {
			keyToDouble.sort(Map.Entry.comparingByKey());

			final DoubleArrayList values = new DoubleArrayList(size);

			keyToDouble.forEach(e -> {
				keys.add(e.getKey());
				values.add(e.getDoubleValue());
			});

			multitypeArrayBuilder.valuesD(DoubleImmutableList.of(values.elements())).valuesType((byte) 2);
		} else {
			// Transfer notObject entries to object case as MultitypeNavigableColumn is mono-type
			keyToObject.addAll(keyToLong);
			keyToObject.addAll(keyToDouble);

			keyToLong.sort(Map.Entry.comparingByKey());

			final ImmutableList.Builder<Object> values = ImmutableList.builderWithExpectedSize(size);

			keyToObject.forEach(e -> {
				keys.add(e.getKey());
				values.add(e.getValue());
			});

			multitypeArrayBuilder.valuesO(values.build()).valuesType((byte) 8);
		}

		return MultitypeNavigableColumn.<T>builder()
				.keys(keys.build())
				.values(multitypeArrayBuilder.build())
				.locked(true)
				.build();
	}
}
