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
package eu.solven.adhoc.dataframe.column.hash;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Streams;

import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.IColumnValueConverter;
import eu.solven.adhoc.cuboid.ICompactable;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.dataframe.IAdhocCapacityConstants;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.encoding.column.AdhocColumnUnsafe;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.primitive.IMultitypeConstants;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongMaps;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
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
public class MultitypeHashColumn<T> implements IMultitypeColumnFastGet<T>, ICompactable {
	// We allow different types through keys. However, this requires a single key to be attached to a single type
	// We do not try aggregating same type together, for a final cross-type aggregation. This could be done in a
	// later/alternative implementation but with unclear benefits. It could actually be done with an additional column
	// with a multiType object
	@Default
	@NonNull
	final Object2LongMap<T> sliceToL = new Object2LongOpenHashMap<>(0);
	@Default
	@NonNull
	final Object2DoubleMap<T> sliceToD = new Object2DoubleOpenHashMap<>(0);
	@Default
	@NonNull
	final Object2ObjectMap<T, Object> sliceToO = new Object2ObjectOpenHashMap<>(0);

	@Default
	protected int capacity = IAdhocCapacityConstants.ZERO_THEN_MAX;

	/**
	 * To be called before a guaranteed `add` operation.
	 */
	protected void checkSizeBeforeAdd(int type) {
		long size = size();

		AdhocColumnUnsafe.checkColumnSize(size);

		if (size == 0) {
			ensureCapacityForType(type);
		}
	}

	@SuppressWarnings({ "PMD.LooseCoupling", "PMD.CollapsibleIfStatements" })
	protected void ensureCapacityForType(int type) {
		if (type == IMultitypeConstants.MASK_LONG) {
			if (sliceToL instanceof Object2LongOpenHashMap<?> openHashMap) {
				openHashMap.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));
			}
		} else if (type == IMultitypeConstants.MASK_DOUBLE) {
			if (sliceToD instanceof Object2DoubleOpenHashMap<?> openHashMap) {
				openHashMap.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));
			}
		} else if (type == IMultitypeConstants.MASK_OBJECT) {
			if (sliceToO instanceof Object2ObjectOpenHashMap<?, ?> openHashMap) {
				openHashMap.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));
			}
		}
	}

	protected boolean containsKey(T key) {
		// `isEmpty` does not really improve performance, but it prevents
		// sliceToValueL from soaking profiling when empty
		if (!sliceToL.isEmpty() && sliceToL.containsKey(key)) {
			return true;
		}
		if (!sliceToD.isEmpty() && sliceToD.containsKey(key)) {
			return true;
		}
		if (!sliceToO.isEmpty() && sliceToO.containsKey(key)) {
			return true;
		}
		return false;
	}

	/**
	 * A put operation: it resets the values for given key, initializing it to the provided value.
	 *
	 * @param key
	 *            if null, this behaves like `.clear`
	 */
	@Override
	public IValueReceiver append(T key) {
		if (containsKey(key)) {
			return merge(key);
		}

		return unsafePut(key, false);
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
	protected IValueReceiver unsafePut(T key, boolean safe) {
		return new IValueReceiver() {
			@Override
			public void onLong(long v) {
				checkSizeBeforeAdd(IMultitypeConstants.MASK_LONG);
				sliceToL.put(key, v);

				if (safe) {
					// measureToAggregateL.removeLong(key);
					sliceToD.removeDouble(key);
					sliceToO.remove(key);
				}
			}

			@Override
			public void onDouble(double v) {
				checkSizeBeforeAdd(IMultitypeConstants.MASK_DOUBLE);
				sliceToD.put(key, v);

				if (safe) {
					sliceToL.removeLong(key);
					sliceToO.remove(key);
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
					sliceToO.put(key, v);

					if (safe) {
						sliceToL.removeLong(key);
						sliceToD.removeDouble(key);
					}
				}
			}
		};
	}

	protected IValueReceiver merge(T key) {
		throw new UnsupportedOperationException("%s can not merge %s".formatted(this, key));
	}

	protected void clearKey(T key) {
		sliceToL.removeLong(key);
		sliceToD.removeDouble(key);
		sliceToO.remove(key);
	}

	@Override
	public IValueProvider onValue(T key) {
		if (sliceToL.containsKey(key)) {
			return vc -> vc.onLong(sliceToL.getLong(key));
		} else if (sliceToD.containsKey(key)) {
			return vc -> vc.onDouble(sliceToD.getDouble(key));
		} else {
			// BEWARE if the key is unknown, the call is done with null
			return vc -> vc.onObject(sliceToO.get(key));
		}
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		// Consider each column is much faster than going with `keySetStream` as
		// it would require searching the column providing given type

		// https://github.com/vigna/fastutil/issues/279
		Object2LongMaps.fastForEach(sliceToL, entry -> {
			rowScanner.onKey(entry.getKey()).onLong(entry.getLongValue());
		});
		Object2DoubleMaps.fastForEach(sliceToD, entry -> {
			rowScanner.onKey(entry.getKey()).onDouble(entry.getDoubleValue());
		});
		Object2ObjectMaps.fastForEach(sliceToO, entry -> {
			rowScanner.onKey(entry.getKey()).onObject(entry.getValue());
		});
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		Stream<U> streamFromLong = Streams.stream(Object2LongMaps.fastIterable(sliceToL))
				.map(entry -> converter.prepare(entry.getKey()).onLong(entry.getLongValue()));
		Stream<U> streamFromDouble = Streams.stream(Object2DoubleMaps.fastIterable(sliceToD))
				.map(entry -> converter.prepare(entry.getKey()).onDouble(entry.getDoubleValue()));
		Stream<U> streamFromObject = Streams.stream(Object2ObjectMaps.fastIterable(sliceToO))
				.map(entry -> converter.prepare(entry.getKey()).onObject(entry.getValue()));

		return Stream.of(streamFromLong, streamFromDouble, streamFromObject).flatMap(Functions.identity());
	}

	@Override
	public Stream<SliceAndMeasure<T>> stream() {
		Stream<SliceAndMeasure<T>> streamFromLong = Streams.stream(Object2LongMaps.fastIterable(sliceToL))
				.map(entry -> SliceAndMeasure.<T>builder()
						.slice(entry.getKey())
						.valueProvider(vc -> vc.onLong(entry.getLongValue()))
						.build());
		Stream<SliceAndMeasure<T>> streamFromDouble = Streams.stream(Object2DoubleMaps.fastIterable(sliceToD))
				.map(entry -> SliceAndMeasure.<T>builder()
						.slice(entry.getKey())
						.valueProvider(vc -> vc.onDouble(entry.getDoubleValue()))
						.build());
		Stream<SliceAndMeasure<T>> streamFromObject = Streams.stream(Object2ObjectMaps.fastIterable(sliceToO))
				.map(entry -> SliceAndMeasure.<T>builder()
						.slice(entry.getKey())
						.valueProvider(vc -> vc.onObject(entry.getValue()))
						.build());

		return Stream.of(streamFromLong, streamFromDouble, streamFromObject).flatMap(Functions.identity());
	}

	@Override
	public long size() {
		long size = 0;

		// Can sum sizes as a key can not appear in multiple columns
		size += sliceToL.size();
		size += sliceToD.size();
		size += sliceToO.size();

		return size;
	}

	@Override
	public boolean isEmpty() {
		if (!sliceToD.isEmpty()) {
			return false;
		} else if (!sliceToL.isEmpty()) {
			return false;
		} else if (!sliceToO.isEmpty()) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public Stream<T> keyStream() {
		return Stream.of(sliceToD.keySet(), sliceToL.keySet(), sliceToO.keySet())
				// No need for .distinct as each key is guaranteed to appear in a single column
				.flatMap(Collection::stream);

	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);

		if (!sliceToD.isEmpty()) {
			toStringHelper.add("#doubles", sliceToD.size());
		}
		if (!sliceToL.isEmpty()) {
			toStringHelper.add("#longs", sliceToL.size());
		}
		if (!sliceToO.isEmpty()) {
			toStringHelper.add("#objects", sliceToO.size());
		}

		AtomicInteger index = new AtomicInteger();
		keyStream().limit(AdhocUnsafe.getLimitOrdinalToString()).forEach(key -> {

			onValue(key).acceptReceiver(o -> {
				toStringHelper.add("#" + index.getAndIncrement() + "-" + key, PepperLogHelper.getObjectAndClass(o));
			});
		});

		return toStringHelper.toString();
	}

	/**
	 * @return an empty and immutable MultiTypeStorage
	 */
	public static <T> MultitypeHashColumn<T> empty() {
		return MultitypeHashColumn.<T>builder()
				.sliceToL(Object2LongMaps.emptyMap())
				.sliceToD(Object2DoubleMaps.emptyMap())
				.sliceToO(Object2ObjectMaps.emptyMap())
				.build();
	}

	@Override
	public MultitypeHashColumn<T> purgeAggregationCarriers() {
		Object2LongMap<T> measureToAggregateL2 = new Object2LongOpenHashMap<>(sliceToL);
		Object2DoubleMap<T> measureToAggregateD2 = new Object2DoubleOpenHashMap<>(sliceToD);
		Object2ObjectMap<T, Object> measureToAggregateO2 = new Object2ObjectOpenHashMap<>(sliceToO);

		MultitypeHashColumn<T> duplicatedForPurge = MultitypeHashColumn.<T>builder()
				.sliceToL(measureToAggregateL2)
				.sliceToD(measureToAggregateD2)
				.sliceToO(measureToAggregateO2)
				.build();

		duplicatedForPurge.unsafePurge();
		return duplicatedForPurge;
	}

	protected void unsafePurge() {
		sliceToO.forEach((key, value) -> {
			if (value instanceof IAggregationCarrier aggregationCarrier) {
				aggregationCarrier.acceptReceiver(new IValueReceiver() {

					@Override
					public void onLong(long value) {
						sliceToL.put(key, value);
						// Removal will happen in a later pass
					}

					@Override
					public void onDouble(double value) {
						sliceToD.put(key, value);
						// Removal will happen in a later pass
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
							// Removal will happen in a later pass
						} else {
							// Replace current value: removal pass will skip this entry as it is not a carrier
							sliceToO.put(key, object);
						}
					}
				});
			}
		});

		// Remove in a later pass, as it is generally unsafe to remove while iterating
		sliceToO.values().removeIf(e -> e instanceof IAggregationCarrier);
	}

	@Override
	@SuppressWarnings("PMD.LooseCoupling")
	public void compact() {
		if (sliceToL instanceof Object2LongOpenHashMap hashMap) {
			hashMap.trim();
		}
		if (sliceToD instanceof Object2DoubleOpenHashMap hashMap) {
			hashMap.trim();
		}
		if (sliceToO instanceof Object2ObjectOpenHashMap hashMap) {
			hashMap.trim();
		}
	}
}
