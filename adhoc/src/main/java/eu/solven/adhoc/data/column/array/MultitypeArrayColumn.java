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
package eu.solven.adhoc.data.column.array;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IColumnValueConverter;
import eu.solven.adhoc.data.column.ICompactable;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeConstants;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Enable storing data with different types, while storing primitive value into primitive arrays.
 *
 * Each key may have different types.
 *
 * @param <T>
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
@Deprecated(since = "Seems not used")
public class MultitypeArrayColumn<T extends Integer> implements IMultitypeColumnFastGet<T>, ICompactable {

	// We allow different types per key. However, this data-structure requires a single key to be attached to a single
	// type
	// We do not try aggregating same type together, for a final cross-type aggregation. This could be done in a
	// later/alternative implementation but with unclear benefits. It could actually be done with an additional column
	// with a multiType object
	@Default
	@NonNull
	final INullableLongList measureToAggregateL = NullableLongList.builder().list(new LongArrayList(0)).build();
	@Default
	@NonNull
	final INullableDoubleList measureToAggregateD = NullableDoubleList.builder().list(new DoubleArrayList(0)).build();
	@Default
	@NonNull
	final INullableObjectList<Object> measureToAggregateO =
			NullableObjectList.builder().list(new ObjectArrayList<>(0)).build();

	/**
	 * To be called before a guaranteed `add` operation.
	 */
	protected void checkSizeBeforeAdd(int type) {
		long size = size();

		AdhocUnsafe.checkColumnSize(size);

		if (size == 0) {
			ensureCapacity(type);
		}
	}

	@SuppressWarnings({ "PMD.LooseCoupling", "PMD.CollapsibleIfStatements" })
	protected void ensureCapacity(int type) {
		// TODO Capacity management does not follow the rest of the code (see MultitypeArray)
		if (type == IMultitypeConstants.MASK_LONG) {
			if (measureToAggregateL instanceof LongArrayList openHashMap) {
				openHashMap.ensureCapacity(AdhocUnsafe.getDefaultColumnCapacity());
			}
		} else if (type == IMultitypeConstants.MASK_DOUBLE) {
			if (measureToAggregateD instanceof DoubleArrayList openHashMap) {
				openHashMap.ensureCapacity(AdhocUnsafe.getDefaultColumnCapacity());
			}
		} else if (type == IMultitypeConstants.MASK_OBJECT) {
			if (measureToAggregateO instanceof ObjectArrayList openHashMap) {
				openHashMap.ensureCapacity(AdhocUnsafe.getDefaultColumnCapacity());
			}
		}
	}

	/**
	 * A put operation: it resets the values for given key, initializing it to the provided value.
	 *
	 * @param key
	 *            if null, this behaves like `.clear`
	 */
	@Override
	public IValueReceiver append(T key) {
		int keyAsInt = key.intValue();
		if (measureToAggregateL.containsIndex(keyAsInt) || measureToAggregateD.containsIndex(keyAsInt)
				|| keyAsInt < measureToAggregateO.size() && measureToAggregateO.get(keyAsInt) != null) {
			return merge(key);
		}

		return unsafePut(key, false);
	}

	@Override
	public IValueReceiver set(T key) {
		return unsafePut(key.intValue(), true);
	}

	/**
	 * BEWARE This is unsafe as it will write without ensuring given key exists only in the provided type. One must
	 * ensure he's not leading to `key` to be present for multi column/types.
	 *
	 * @param key
	 * @param safe
	 *            if true, we request safe behavior: it will ensure given key will not be associated to multiple types.
	 *            Can be false if you know this call is not changing the column type (e.g. reading as long, and writing
	 *            a long).
	 * @return
	 */
	protected IValueReceiver unsafePut(int key, boolean safe) {
		return new IValueReceiver() {
			@Override
			public void onLong(long v) {
				checkSizeBeforeAdd(IMultitypeConstants.MASK_LONG);

				measureToAggregateL.set(key, v);

				if (safe) {
					// measureToAggregateL.removeLong(key);
					measureToAggregateD.removeDouble(key);
					measureToAggregateO.remove(key);
				}
			}

			@Override
			public void onDouble(double v) {
				checkSizeBeforeAdd(IMultitypeConstants.MASK_DOUBLE);
				measureToAggregateD.set(key, v);

				if (safe) {
					measureToAggregateL.removeLong(key);
					measureToAggregateO.remove(key);
				}
			}

			@Override
			public void onObject(Object v) {
				if (AdhocPrimitiveHelpers.isLongLike(v)) {
					long vAsPrimitive = AdhocPrimitiveHelpers.asLong(v);
					onLong(vAsPrimitive);
				} else if (AdhocPrimitiveHelpers.isDoubleLike(v)) {
					double vAsPrimitive = AdhocPrimitiveHelpers.asDouble(v);
					onDouble(vAsPrimitive);
				} else if (v != null) {
					checkSizeBeforeAdd(IMultitypeConstants.MASK_OBJECT);
					measureToAggregateO.set(key, v);

					if (safe) {
						measureToAggregateL.removeLong(key);
						measureToAggregateD.removeDouble(key);
					}
				}
			}
		};
	}

	protected IValueReceiver merge(T key) {
		throw new UnsupportedOperationException("%s can not merge %s".formatted(this, key));
	}

	protected void clearKey(T key) {
		measureToAggregateL.removeLong(key);
		measureToAggregateD.removeDouble(key);
		measureToAggregateO.remove(key);
	}

	@Override
	public IValueProvider onValue(T key) {
		int keyAsInt = key.intValue();
		if (measureToAggregateL.containsIndex(keyAsInt)) {
			return vc -> vc.onLong(measureToAggregateL.getLong(keyAsInt));
		} else if (measureToAggregateD.containsIndex(key)) {
			return vc -> vc.onDouble(measureToAggregateD.getDouble(keyAsInt));
		} else {
			// BEWARE if the key is unknown, the call is done with null
			return vc -> vc.onObject(measureToAggregateO.get(keyAsInt));
		}
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		// Consider each column is much faster than going with `keySetStream` as
		// it would require searching the column providing given type

		// https://github.com/vigna/fastutil/issues/279
		for (int i = 0; i < measureToAggregateL.size(); i++) {
			rowScanner.onKey(toBoxedKey(i)).onLong(measureToAggregateL.getLong(i));
		}
		for (int i = 0; i < measureToAggregateD.size(); i++) {
			rowScanner.onKey(toBoxedKey(i)).onDouble(measureToAggregateD.getDouble(i));
		}
		for (int i = 0; i < measureToAggregateO.size(); i++) {
			rowScanner.onKey(toBoxedKey(i)).onObject(measureToAggregateO.get(i));
		}
	}

	@SuppressWarnings("PMD.UnnecessaryBoxing")
	protected T toBoxedKey(int i) {
		return (T) Integer.valueOf(i);
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		Stream<U> streamFromLong = measureToAggregateL.indexStream()
				.mapToObj(i -> converter.prepare(toBoxedKey(i)).onLong(measureToAggregateL.getLong(i)));
		Stream<U> streamFromDouble = measureToAggregateD.indexStream()
				.mapToObj(i -> converter.prepare(toBoxedKey(i)).onDouble(measureToAggregateD.getDouble(i)));
		Stream<U> streamFromObject = objectIndexStream()
				.mapToObj(i -> converter.prepare(toBoxedKey(i)).onObject(measureToAggregateO.get(i)));

		return Stream.of(streamFromLong, streamFromDouble, streamFromObject).flatMap(Functions.identity());
	}

	private IntStream objectIndexStream() {
		return IntStream.range(0, measureToAggregateO.size()).filter(i -> measureToAggregateO.get(i) != null);
	}

	@Override
	public Stream<SliceAndMeasure<T>> stream() {
		Stream<SliceAndMeasure<T>> streamFromLong = IntStream.range(0, measureToAggregateL.size())
				.mapToObj(i -> SliceAndMeasure.<T>builder()
						.slice(toBoxedKey(i))
						.valueProvider(vc -> vc.onLong(measureToAggregateL.getLong(i)))
						.build());

		Stream<SliceAndMeasure<T>> streamFromDouble = IntStream.range(0, measureToAggregateD.size())
				.mapToObj(i -> SliceAndMeasure.<T>builder()
						.slice(toBoxedKey(i))
						.valueProvider(vc -> vc.onDouble(measureToAggregateD.getDouble(i)))
						.build());

		Stream<SliceAndMeasure<T>> streamFromObject = objectIndexStream().mapToObj(i -> SliceAndMeasure.<T>builder()
				.slice(toBoxedKey(i))
				.valueProvider(vc -> vc.onObject(measureToAggregateO.get(i)))
				.build());

		return Stream.of(streamFromLong, streamFromDouble, streamFromObject).flatMap(Functions.identity());
	}

	@Override
	public long size() {
		long size = 0;

		// Can sum sizes as a key can not appear in multiple columns
		size += measureToAggregateL.sizeNotNull();
		size += measureToAggregateD.sizeNotNull();
		size += measureToAggregateO.sizeNotNull();

		return size;
	}

	@Override
	public boolean isEmpty() {
		if (!measureToAggregateD.isEmpty()) {
			return false;
		} else if (!measureToAggregateL.isEmpty()) {
			return false;
		} else if (!measureToAggregateO.isEmpty()) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public Stream<T> keyStream() {
		return Stream.of(measureToAggregateL.indexStream(), measureToAggregateD.indexStream(), objectIndexStream())
				.flatMapToInt(is -> is)
				.mapToObj(this::toBoxedKey)
		// No need for .distinct as each key is guaranteed to appear in a single column
		// .distinct()
		;

	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);

		if (!measureToAggregateD.isEmpty()) {
			toStringHelper.add("#doubles", measureToAggregateD.sizeNotNull());
		}
		if (!measureToAggregateL.isEmpty()) {
			toStringHelper.add("#longs", measureToAggregateL.sizeNotNull());
		}
		if (!measureToAggregateO.isEmpty()) {
			toStringHelper.add("#objects", measureToAggregateO.sizeNotNull());
		}

		AtomicInteger index = new AtomicInteger();
		keyStream().limit(AdhocUnsafe.getLimitOrdinalToString()).forEach(key -> {

			onValue(key).acceptReceiver(o -> toStringHelper.add("#" + index.getAndIncrement() + "-" + key,
					PepperLogHelper.getObjectAndClass(o)));
		});

		return toStringHelper.toString();
	}

	/**
	 * @return an empty and immutable MultiTypeStorage
	 */
	public static <T extends Integer> MultitypeArrayColumn<T> empty() {
		return MultitypeArrayColumn.<T>builder()
				.measureToAggregateL(NullableLongList.empty())
				.measureToAggregateD(NullableDoubleList.empty())
				.measureToAggregateO(NullableObjectList.empty())
				.build();
	}

	@Override
	public MultitypeArrayColumn<T> purgeAggregationCarriers() {
		final INullableLongList measureToAggregateL2 = measureToAggregateL.duplicate();
		final INullableDoubleList measureToAggregateD2 = measureToAggregateD.duplicate();
		final INullableObjectList<Object> measureToAggregateO2 = measureToAggregateO.duplicate();

		MultitypeArrayColumn<T> duplicatedForPurge = MultitypeArrayColumn.<T>builder()
				.measureToAggregateL(measureToAggregateL2)
				.measureToAggregateD(measureToAggregateD2)
				.measureToAggregateO(measureToAggregateO2)
				.build();

		duplicatedForPurge.unsafePurge();
		return duplicatedForPurge;
	}

	protected void unsafePurge() {
		for (int keyNotFinal = 0; keyNotFinal < measureToAggregateO.size(); keyNotFinal++) {
			int key = keyNotFinal;
			Object value = measureToAggregateO.get(key);

			if (value instanceof IAggregationCarrier aggregationCarrier) {
				aggregationCarrier.acceptValueReceiver(new IValueReceiver() {

					@Override
					public void onLong(long value) {
						measureToAggregateL.set(key, value);
						measureToAggregateO.set(key, null);
					}

					@Override
					public void onDouble(double value) {
						measureToAggregateD.set(key, value);
						measureToAggregateO.set(key, null);
					}

					@Override
					public void onObject(Object object) {
						if (object instanceof IAggregationCarrier aggregationCarrier) {
							throw new IllegalArgumentException(
									"Illegal purge from %s to %s".formatted(aggregationCarrier, object));
						} else if (object == null) {
							// `object` may be null while carrier was not null
							// (e.g. `Rank2` while we received only one value)
							log.trace("Skipping `null` from carrier for key={}", key);
							measureToAggregateO.set(key, null);
						} else {
							// Replace current value: removal pass will skip this entry as it is not a carrier
							measureToAggregateO.set(key, object);
						}
					}
				});
			}
		}

		// TODO Typically on CountAggregation, we turned all Carriers into a long. So `measureToAggregateO` holds only
		// `null`, and it could be cleared.
	}

	@Override
	public void compact() {
		if (measureToAggregateL instanceof ICompactable compactable) {
			compactable.compact();
		}
		if (measureToAggregateD instanceof ICompactable compactable) {
			compactable.compact();
		}
		if (measureToAggregateO instanceof ICompactable compactable) {
			compactable.compact();
		}
	}
}
