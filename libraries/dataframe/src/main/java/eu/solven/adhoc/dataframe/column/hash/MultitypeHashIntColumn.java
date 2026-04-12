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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Streams;

import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.IColumnValueConverter;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.dataframe.IAdhocCapacityConstants;
import eu.solven.adhoc.dataframe.column.IMultitypeIntColumnFastGet;
import eu.solven.adhoc.encoding.column.AdhocColumnUnsafe;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.primitive.IMultitypeConstants;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.stream.ConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMaps;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Enable storing data with different types, while storing primitive value into primitive arrays.
 *
 * Each key may have different types.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class MultitypeHashIntColumn implements IMultitypeIntColumnFastGet, ICompactable {
	// We allow different types through keys. However, this requires a single key to be attached to a single type
	// We do not try aggregating same type together, for a final cross-type aggregation. This could be done in a
	// later/alternative implementation but with unclear benefits. It could actually be done with an additional column
	// with a multiType object
	@Default
	@NonNull
	final Int2LongMap sliceToL = new Int2LongOpenHashMap(0);
	@Default
	@NonNull
	final Int2DoubleMap sliceToD = new Int2DoubleOpenHashMap(0);
	@Default
	@NonNull
	final Int2ObjectMap<Object> sliceToO = new Int2ObjectOpenHashMap<>(0);

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
			if (sliceToL instanceof Int2LongOpenHashMap openHashMap) {
				openHashMap.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));
			}
		} else if (type == IMultitypeConstants.MASK_DOUBLE) {
			if (sliceToD instanceof Int2DoubleOpenHashMap openHashMap) {
				openHashMap.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));
			}
		} else if (type == IMultitypeConstants.MASK_OBJECT) {
			if (sliceToO instanceof Int2ObjectOpenHashMap<?> openHashMap) {
				openHashMap.ensureCapacity(IAdhocCapacityConstants.capacity(capacity));
			}
		}
	}

	protected boolean containsKey(int key) {
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
	public IValueReceiver append(int key) {
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
	protected IValueReceiver unsafePut(int key, boolean safe) {
		return new IValueReceiver() {
			@Override
			public void onLong(long v) {
				checkSizeBeforeAdd(IMultitypeConstants.MASK_LONG);
				sliceToL.put(key, v);

				if (safe) {
					// measureToAggregateL.removeLong(key);
					sliceToD.remove(key);
					sliceToO.remove(key);
				}
			}

			@Override
			public void onDouble(double v) {
				checkSizeBeforeAdd(IMultitypeConstants.MASK_DOUBLE);
				sliceToD.put(key, v);

				if (safe) {
					sliceToL.remove(key);
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
						sliceToL.remove(key);
						sliceToD.remove(key);
					}
				}
			}
		};
	}

	protected IValueReceiver merge(int key) {
		throw new UnsupportedOperationException("%s can not merge %s".formatted(this, key));
	}

	protected void clearKey(int key) {
		sliceToL.remove(key);
		sliceToD.remove(key);
		sliceToO.remove(key);
	}

	@Override
	public IValueProvider onValue(int key) {
		if (sliceToL.containsKey(key)) {
			return vc -> vc.onLong(sliceToL.get(key));
		} else if (sliceToD.containsKey(key)) {
			return vc -> vc.onDouble(sliceToD.get(key));
		} else {
			// BEWARE if the key is unknown, the call is done with null
			return vc -> vc.onObject(sliceToO.get(key));
		}
	}

	@Override
	public void scan(IColumnScanner<Integer> rowScanner) {
		// Consider each column is much faster than going with `keySetStream` as
		// it would require searching the column providing given type

		// https://github.com/vigna/fastutil/issues/279
		Int2LongMaps.fastForEach(sliceToL, e -> rowScanner.onKey(e.getIntKey()).onLong(e.getLongValue()));
		Int2DoubleMaps.fastForEach(sliceToD, e -> rowScanner.onKey(e.getIntKey()).onDouble(e.getDoubleValue()));
		Int2ObjectMaps.fastForEach(sliceToO, e -> rowScanner.onKey(e.getIntKey()).onObject(e.getValue()));
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<Integer, U> converter) {
		Stream<U> streamFromLong = Streams.stream(Int2LongMaps.fastIterable(sliceToL))
				.map(entry -> converter.prepare(entry.getIntKey()).onLong(entry.getLongValue()));
		Stream<U> streamFromDouble = Streams.stream(Int2DoubleMaps.fastIterable(sliceToD))
				.map(entry -> converter.prepare(entry.getIntKey()).onDouble(entry.getDoubleValue()));
		Stream<U> streamFromObject = Streams.stream(Int2ObjectMaps.fastIterable(sliceToO))
				.map(entry -> converter.prepare(entry.getIntKey()).onObject(entry.getValue()));

		return Stream.of(streamFromLong, streamFromDouble, streamFromObject).flatMap(Functions.identity());
	}

	@Override
	public IConsumingStream<SliceAndMeasure<Integer>> stream() {
		return ConsumingStream.<SliceAndMeasure<Integer>>builder().source(consumer -> {
			sliceToL.forEach((slice, o) -> consumer
					.accept(SliceAndMeasure.<Integer>builder().slice(slice).valueProvider(vc -> vc.onLong(o)).build()));
			sliceToD.forEach((slice, o) -> consumer.accept(
					SliceAndMeasure.<Integer>builder().slice(slice).valueProvider(vc -> vc.onDouble(o)).build()));
			sliceToO.forEach((slice, o) -> consumer.accept(
					SliceAndMeasure.<Integer>builder().slice(slice).valueProvider(vc -> vc.onObject(o)).build()));
		}).build();
	}

	@Override
	public IConsumingStream<SliceAndMeasure<Integer>> limit(int limit) {
		throw new NotYetImplementedException("Needed?");
	}

	@Override
	public IConsumingStream<SliceAndMeasure<Integer>> skip(int skip) {
		throw new NotYetImplementedException("Needed?");
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

	@SuppressWarnings("CPD-START")
	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public IConsumingStream<Integer> keyStream() {
		// No need for .distinct as each key is guaranteed to appear in a single column
		return ConsumingStream.<Integer>builder().source(consumer -> {
			sliceToL.keySet().intStream().forEach(consumer::accept);
			sliceToD.keySet().intStream().forEach(consumer::accept);
			sliceToO.keySet().intStream().forEach(consumer::accept);
		}).build();

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
		keyStream().toList().stream().limit(AdhocUnsafe.getLimitOrdinalToString()).forEach(key -> {

			onValue(key).acceptReceiver(o -> {
				toStringHelper.add("#" + index.getAndIncrement() + "-" + key, PepperLogHelper.getObjectAndClass(o));
			});
		});

		return toStringHelper.toString();
	}

	/**
	 * @return an empty and immutable MultiTypeStorage
	 */
	public static MultitypeHashIntColumn empty() {
		return MultitypeHashIntColumn.builder()
				.sliceToL(Int2LongMaps.EMPTY_MAP)
				.sliceToD(Int2DoubleMaps.EMPTY_MAP)
				.sliceToO(Int2ObjectMaps.emptyMap())
				.build();
	}

	@Override
	public MultitypeHashIntColumn purgeAggregationCarriers() {
		Int2LongMap measureToAggregateL2 = new Int2LongOpenHashMap(sliceToL);
		Int2DoubleMap measureToAggregateD2 = new Int2DoubleOpenHashMap(sliceToD);
		Int2ObjectMap<Object> measureToAggregateO2 = new Int2ObjectOpenHashMap<>(sliceToO);

		MultitypeHashIntColumn duplicatedForPurge = MultitypeHashIntColumn.builder()
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
		if (sliceToL instanceof Int2LongOpenHashMap hashMap) {
			hashMap.trim();
		}
		if (sliceToD instanceof Int2DoubleOpenHashMap hashMap) {
			hashMap.trim();
		}
		if (sliceToO instanceof Int2ObjectOpenHashMap hashMap) {
			hashMap.trim();
		}
	}
}
