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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Streams;

import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.storage.IValueConsumer;
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
 * @param <T>
 */
@SuperBuilder
@Slf4j
public class MultitypeHashColumn<T> implements IMultitypeColumnFastGet<T> {

	// We allow different types per key. However, this data-structure requires a single key to be attached to a single
	// type
	// We do not try aggregating same type together, for a final cross-type aggregation. This could be done in a
	// later/alternative
	// implementation but with unclear benefits. It could actually be done with an additional column with a multiType
	// object
	@Default
	@NonNull
	final Object2DoubleMap<T> measureToAggregateD = new Object2DoubleOpenHashMap<>();
	@Default
	@NonNull
	final Object2LongMap<T> measureToAggregateL = new Object2LongOpenHashMap<>();
	@Default
	@NonNull
	final Object2ObjectMap<T, String> measureToAggregateS = new Object2ObjectOpenHashMap<>();
	@Default
	@NonNull
	final Object2ObjectMap<T, Object> measureToAggregateO = new Object2ObjectOpenHashMap<>();

	/**
	 * A put operation: it resets the values for given key, initializing it to the provided value.
	 *
	 * @param key
	 * @param v
	 *            if null, this behave like `.clear`
	 */
	@Override
	public IValueConsumer append(T key) {
		// We clear all keys, to prevent storing different types for the same key
		// clearKey(key);

		if (measureToAggregateL.containsKey(key) || measureToAggregateD.containsKey(key)
				|| measureToAggregateS.containsKey(key)
				|| measureToAggregateO.containsKey(key)) {
			return merge(key);
		}

		return unsafePut(key);
	}

	/**
	 * BEWARE This is unsafe as it will write without ensuring given key exists only in the provided type
	 *
	 * @param key
	 * @return
	 */
	protected IValueConsumer unsafePut(T key) {
		return new IValueConsumer() {
			@Override
			public void onLong(long v) {
				long vAsPrimitive = SumAggregation.asLong(v);
				measureToAggregateL.put(key, vAsPrimitive);
			}

			@Override
			public void onDouble(double v) {
				double vAsPrimitive = SumAggregation.asDouble(v);
				measureToAggregateD.put(key, vAsPrimitive);
			}

			@Override
			public void onCharsequence(CharSequence v) {
				String vAsString = v.toString();
				measureToAggregateS.put(key, vAsString);
			}

			@Override
			public void onObject(Object v) {
				if (SumAggregation.isLongLike(v)) {
					long vAsPrimitive = SumAggregation.asLong(v);
					measureToAggregateL.put(key, vAsPrimitive);
				} else if (SumAggregation.isDoubleLike(v)) {
					double vAsPrimitive = SumAggregation.asDouble(v);
					measureToAggregateD.put(key, vAsPrimitive);
				} else if (v instanceof CharSequence) {
					String vAsString = v.toString();
					measureToAggregateS.put(key, vAsString);
				} else if (v != null) {
					measureToAggregateO.put(key, v);
				}
			}
		};
	}

	protected IValueConsumer merge(T key) {
		throw new UnsupportedOperationException("%s can not merge %s".formatted(this, key));
	}

	protected void clearKey(T key) {
		measureToAggregateL.removeLong(key);
		measureToAggregateD.removeDouble(key);
		measureToAggregateS.remove(key);
		measureToAggregateO.remove(key);
	}

	@Override
	public void onValue(T key, IValueConsumer consumer) {
		if (measureToAggregateL.containsKey(key)) {
			consumer.onLong(measureToAggregateL.getLong(key));
		} else if (measureToAggregateD.containsKey(key)) {
			consumer.onDouble(measureToAggregateD.getDouble(key));
		} else if (measureToAggregateS.containsKey(key)) {
			consumer.onCharsequence(measureToAggregateS.get(key));
		} else {
			// BEWARE if the key is unknown, the call is done with null
			consumer.onObject(measureToAggregateO.get(key));
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
		Object2ObjectMaps.fastForEach(measureToAggregateS, entry -> {
			rowScanner.onKey(entry.getKey()).onCharsequence(entry.getValue());
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
		Stream<U> streamFromCharsequence = Streams.stream(Object2ObjectMaps.fastIterable(measureToAggregateS))
				.map(entry -> converter.prepare(entry.getKey()).onCharsequence(entry.getValue()));
		Stream<U> streamFromObject = Streams.stream(Object2ObjectMaps.fastIterable(measureToAggregateO))
				.map(entry -> converter.prepare(entry.getKey()).onObject(entry.getValue()));

		return Stream.of(streamFromLong, streamFromDouble, streamFromCharsequence, streamFromObject)
				.flatMap(Functions.identity());
	}

	@Override
	public Stream<SliceAndMeasure<T>> stream() {
		Stream<SliceAndMeasure<T>> streamFromLong = Streams.stream(Object2LongMaps.fastIterable(measureToAggregateL))
				.map(entry -> SliceAndMeasure.<T>builder()
						.slice(entry.getKey())
						.valueConsumerConsumer(vc -> vc.onLong(entry.getLongValue()))
						.build());
		Stream<SliceAndMeasure<T>> streamFromDouble =
				Streams.stream(Object2DoubleMaps.fastIterable(measureToAggregateD))
						.map(entry -> SliceAndMeasure.<T>builder()
								.slice(entry.getKey())
								.valueConsumerConsumer(vc -> vc.onDouble(entry.getDoubleValue()))
								.build());
		Stream<SliceAndMeasure<T>> streamFromCharsequence =
				Streams.stream(Object2ObjectMaps.fastIterable(measureToAggregateS))
						.map(entry -> SliceAndMeasure.<T>builder()
								.slice(entry.getKey())
								.valueConsumerConsumer(vc -> vc.onCharsequence(entry.getValue()))
								.build());
		Stream<SliceAndMeasure<T>> streamFromObject =
				Streams.stream(Object2ObjectMaps.fastIterable(measureToAggregateO))
						.map(entry -> SliceAndMeasure.<T>builder()
								.slice(entry.getKey())
								.valueConsumerConsumer(vc -> vc.onObject(entry.getValue()))
								.build());

		return Stream.of(streamFromLong, streamFromDouble, streamFromCharsequence, streamFromObject)
				.flatMap(Functions.identity());
	}

	@Override
	public long size() {
		long size = 0;

		// Can sum sizes as a key can not appear in multiple columns
		size += measureToAggregateL.size();
		size += measureToAggregateD.size();
		size += measureToAggregateS.size();
		size += measureToAggregateO.size();

		return size;
	}

	@Override
	public boolean isEmpty() {
		if (!measureToAggregateD.isEmpty()) {
			return false;
		} else if (!measureToAggregateL.isEmpty()) {
			return false;
		} else if (!measureToAggregateS.isEmpty()) {
			return false;
		} else if (!measureToAggregateO.isEmpty()) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public Stream<T> keyStream() {
		return Stream
				.of(measureToAggregateD.keySet(),
						measureToAggregateL.keySet(),
						measureToAggregateS.keySet(),
						measureToAggregateO.keySet())
				// No need for .distinct as each key is guaranteed to appear in a single column
				.flatMap(Collection::stream);

	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);

		if (!measureToAggregateD.isEmpty()) {
			toStringHelper.add("measureToAggregateD.size()", measureToAggregateD.size());
		}
		if (!measureToAggregateL.isEmpty()) {
			toStringHelper.add("measureToAggregateL.size()", measureToAggregateL.size());
		}
		if (!measureToAggregateS.isEmpty()) {
			toStringHelper.add("measureToAggregateS.size()", measureToAggregateS.size());
		}
		if (!measureToAggregateO.isEmpty()) {
			toStringHelper.add("measureToAggregateO.size()", measureToAggregateO.size());
		}

		AtomicInteger index = new AtomicInteger();
		keyStream().limit(AdhocUnsafe.limitOrdinalToString).forEach(key -> {

			onValue(key, o -> {
				toStringHelper.add("#" + index.getAndIncrement(), PepperLogHelper.getObjectAndClass(o));
			});
		});

		return toStringHelper.toString();
	}

	/**
	 * @return an empty and immutable MultiTypeStorage
	 */
	public static <T> MultitypeHashColumn<T> empty() {
		return MultitypeHashColumn.<T>builder()
				.measureToAggregateD(Object2DoubleMaps.emptyMap())
				.measureToAggregateL(Object2LongMaps.emptyMap())
				.measureToAggregateS(Object2ObjectMaps.emptyMap())
				.measureToAggregateO(Object2ObjectMaps.emptyMap())
				.build();
	}

	@Override
	public void purgeAggregationCarriers() {
		// We collect entries to remove, not to modify `measureToAggregateO` while iterating over it
		List<T> toRemove = new ArrayList<>();

		measureToAggregateO.forEach((key, value) -> {
			if (value instanceof IAggregationCarrier aggregationCarrier) {
				aggregationCarrier.acceptValueConsumer(new IValueConsumer() {

					@Override
					public void onLong(long value) {
						measureToAggregateL.put(key, value);
						toRemove.add(key);
					}

					@Override
					public void onDouble(double value) {
						measureToAggregateD.put(key, value);
						toRemove.add(key);
					}

					@Override
					public void onCharsequence(CharSequence value) {
						measureToAggregateS.put(key, value.toString());
						toRemove.add(key);
					}

					@Override
					public void onObject(Object object) {
						// Replace current value
						measureToAggregateO.put(key, value);
					}
				});
			}
		});

		toRemove.forEach(measureToAggregateO::remove);
	}
}
