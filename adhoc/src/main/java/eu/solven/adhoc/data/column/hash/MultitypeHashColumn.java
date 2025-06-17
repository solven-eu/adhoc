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
package eu.solven.adhoc.data.column.hash;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Streams;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IColumnValueConverter;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeConstants;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
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
public class MultitypeHashColumn<T> implements IMultitypeColumnFastGet<T> {
	// We allow different types per key. However, this data-structure requires a single key to be attached to a single
	// type
	// We do not try aggregating same type together, for a final cross-type aggregation. This could be done in a
	// later/alternative implementation but with unclear benefits. It could actually be done with an additional column
	// with a multiType object
	@Default
	@NonNull
	final Object2LongMap<T> measureToAggregateL = new Object2LongOpenHashMap<>(0);
	@Default
	@NonNull
	final Object2DoubleMap<T> measureToAggregateD = new Object2DoubleOpenHashMap<>(0);
	@Default
	@NonNull
	final Object2ObjectMap<T, Object> measureToAggregateO = new Object2ObjectOpenHashMap<>(0);

	/**
	 * To be called before a guaranteed `add` operation.
	 */
	protected void checkSizeBeforeAdd(int type) {
		long size = size();
		if (size >= AdhocUnsafe.limitColumnSize) {
			// TODO Log the first and last elements
			throw new IllegalStateException(
					"Can not add as size=%s and limit=%s".formatted(size, AdhocUnsafe.limitColumnSize));
		} else if (size == 0) {
			ensureCapacity(type);
		}
	}

	@SuppressWarnings({ "PMD.LooseCoupling", "PMD.CollapsibleIfStatements" })
	protected void ensureCapacity(int type) {
		if (type == IMultitypeConstants.MASK_LONG) {
			if (measureToAggregateL instanceof Object2LongOpenHashMap<?> openHashMap) {
				openHashMap.ensureCapacity(AdhocUnsafe.defaultColumnCapacity());
			}
		} else if (type == IMultitypeConstants.MASK_DOUBLE) {
			if (measureToAggregateD instanceof Object2DoubleOpenHashMap<?> openHashMap) {
				openHashMap.ensureCapacity(AdhocUnsafe.defaultColumnCapacity());
			}
		} else if (type == IMultitypeConstants.MASK_OBJECT) {
			if (measureToAggregateO instanceof Object2ObjectOpenHashMap<?, ?> openHashMap) {
				openHashMap.ensureCapacity(AdhocUnsafe.defaultColumnCapacity());
			}
		}
	}

	protected boolean containsKey(T key) {
		return measureToAggregateL.containsKey(key) || measureToAggregateD.containsKey(key)
				|| measureToAggregateO.containsKey(key);
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

	@Override
	public IValueReceiver set(T key) {
		return unsafePut(key, true);
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
				measureToAggregateL.put(key, v);

				if (safe) {
					// measureToAggregateL.removeLong(key);
					measureToAggregateD.removeDouble(key);
					measureToAggregateO.remove(key);
				}
			}

			@Override
			public void onDouble(double v) {
				checkSizeBeforeAdd(IMultitypeConstants.MASK_DOUBLE);
				measureToAggregateD.put(key, v);

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
					measureToAggregateO.put(key, v);

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
	public void onValue(T key, IValueReceiver consumer) {
		if (measureToAggregateL.containsKey(key)) {
			consumer.onLong(measureToAggregateL.getLong(key));
		} else if (measureToAggregateD.containsKey(key)) {
			consumer.onDouble(measureToAggregateD.getDouble(key));
		} else {
			// BEWARE if the key is unknown, the call is done with null
			Object value = measureToAggregateO.get(key);
			// if (value == null) {
			// consumer.onNull();
			// } else {
			consumer.onObject(value);
			// }
		}
	}

	@Override
	public IValueProvider onValue(T key) {
		if (measureToAggregateL.containsKey(key)) {
			return vc -> vc.onLong(measureToAggregateL.getLong(key));
		} else if (measureToAggregateD.containsKey(key)) {
			return vc -> vc.onDouble(measureToAggregateD.getDouble(key));
		} else {
			// BEWARE if the key is unknown, the call is done with null
			return vc -> vc.onObject(measureToAggregateO.get(key));
		}
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		// Consider each column is much faster than going with `keySetStream` as
		// it would require searching the column providing given type

		// https://github.com/vigna/fastutil/issues/279
		Object2LongMaps.fastForEach(measureToAggregateL, entry -> {
			rowScanner.onKey(entry.getKey()).onLong(entry.getLongValue());
		});
		Object2DoubleMaps.fastForEach(measureToAggregateD, entry -> {
			rowScanner.onKey(entry.getKey()).onDouble(entry.getDoubleValue());
		});
		Object2ObjectMaps.fastForEach(measureToAggregateO, entry -> {
			rowScanner.onKey(entry.getKey()).onObject(entry.getValue());
		});
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		Stream<U> streamFromLong = Streams.stream(Object2LongMaps.fastIterable(measureToAggregateL))
				.map(entry -> converter.prepare(entry.getKey()).onLong(entry.getLongValue()));
		Stream<U> streamFromDouble = Streams.stream(Object2DoubleMaps.fastIterable(measureToAggregateD))
				.map(entry -> converter.prepare(entry.getKey()).onDouble(entry.getDoubleValue()));
		Stream<U> streamFromObject = Streams.stream(Object2ObjectMaps.fastIterable(measureToAggregateO))
				.map(entry -> converter.prepare(entry.getKey()).onObject(entry.getValue()));

		return Stream.of(streamFromLong, streamFromDouble, streamFromObject).flatMap(Functions.identity());
	}

	@Override
	public Stream<SliceAndMeasure<T>> stream() {
		Stream<SliceAndMeasure<T>> streamFromLong = Streams.stream(Object2LongMaps.fastIterable(measureToAggregateL))
				.map(entry -> SliceAndMeasure.<T>builder()
						.slice(entry.getKey())
						.valueProvider(vc -> vc.onLong(entry.getLongValue()))
						.build());
		Stream<SliceAndMeasure<T>> streamFromDouble =
				Streams.stream(Object2DoubleMaps.fastIterable(measureToAggregateD))
						.map(entry -> SliceAndMeasure.<T>builder()
								.slice(entry.getKey())
								.valueProvider(vc -> vc.onDouble(entry.getDoubleValue()))
								.build());
		Stream<SliceAndMeasure<T>> streamFromObject =
				Streams.stream(Object2ObjectMaps.fastIterable(measureToAggregateO))
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
		size += measureToAggregateL.size();
		size += measureToAggregateD.size();
		size += measureToAggregateO.size();

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
		return Stream.of(measureToAggregateD.keySet(), measureToAggregateL.keySet(), measureToAggregateO.keySet())
				// No need for .distinct as each key is guaranteed to appear in a single column
				.flatMap(Collection::stream);

	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);

		if (!measureToAggregateD.isEmpty()) {
			toStringHelper.add("#doubles", measureToAggregateD.size());
		}
		if (!measureToAggregateL.isEmpty()) {
			toStringHelper.add("#longs", measureToAggregateL.size());
		}
		if (!measureToAggregateO.isEmpty()) {
			toStringHelper.add("#objects", measureToAggregateO.size());
		}

		AtomicInteger index = new AtomicInteger();
		keyStream().limit(AdhocUnsafe.limitOrdinalToString).forEach(key -> {

			onValue(key, o -> {
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
				.measureToAggregateL(Object2LongMaps.emptyMap())
				.measureToAggregateD(Object2DoubleMaps.emptyMap())
				.measureToAggregateO(Object2ObjectMaps.emptyMap())
				.build();
	}

	@Override
	public void purgeAggregationCarriers() {
		measureToAggregateO.forEach((key, value) -> {
			if (value instanceof IAggregationCarrier aggregationCarrier) {
				aggregationCarrier.acceptValueReceiver(new IValueReceiver() {

					@Override
					public void onLong(long value) {
						measureToAggregateL.put(key, value);
						// Removal will happen in a later pass
					}

					@Override
					public void onDouble(double value) {
						measureToAggregateD.put(key, value);
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
							measureToAggregateO.put(key, object);
						}
					}
				});
			}
		});

		// Remove in a later pass, as it is generally unsafe to remove while iterating
		measureToAggregateO.values().removeIf(e -> e instanceof IAggregationCarrier);
	}
}
