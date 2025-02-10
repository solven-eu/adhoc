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
package eu.solven.adhoc.storage;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Streams;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.SumAggregator;
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
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * This data-structures aggregates input value on a per-key basis. Different keys are allowed to be associated to
 * different types (e.g. we may have some keys holding a functional double, while other keys may hold an error String).
 * <p>
 * This data-structure does not maintain order. Typically `SUM(123, 'a', 234)` could lead to `'123a234'` or `'357a'`.
 *
 * @param <T>
 */
@Builder
@Slf4j
public class MultiTypeStorage<T> {

	@Default
	@NonNull
	IAggregation aggregation = new SumAggregator();

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
	public void put(T key, Object v) {
		// We clear all keys, to prevent storing different types for the same key
		clearKey(key);

		if (SumAggregator.isLongLike(v)) {
			long vAsPrimitive = SumAggregator.asLong(v);
			measureToAggregateL.put(key, vAsPrimitive);
		} else if (SumAggregator.isDoubleLike(v)) {
			double vAsPrimitive = SumAggregator.asDouble(v);
			measureToAggregateD.put(key, vAsPrimitive);
		} else if (v instanceof CharSequence) {
			String vAsString = v.toString();
			measureToAggregateS.put(key, vAsString);
		} else if (v != null) {
			// throw new UnsupportedOperationException("Received: %s".formatted(PepperLogHelper.getObjectAndClass(v)));
			measureToAggregateO.put(key, v);
		}
	}

	protected void clearKey(T key) {
		measureToAggregateL.removeLong(key);
		measureToAggregateD.removeDouble(key);
		measureToAggregateS.remove(key);
		measureToAggregateO.remove(key);
	}

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

	public void scan(IRowScanner<T> rowScanner) {
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

	public <U> Stream<U> stream(IRowConverter<T, U> converter) {
		Stream<U> streamFromLong = Streams.stream(Object2LongMaps.fastIterable(measureToAggregateL))
				.map(entry -> converter.convertLong(entry.getKey(), entry.getLongValue()));
		Stream<U> streamFromDouble = Streams.stream(Object2DoubleMaps.fastIterable(measureToAggregateD))
				.map(entry -> converter.convertDouble(entry.getKey(), entry.getDoubleValue()));
		Stream<U> streamFromCharsequence = Streams.stream(Object2ObjectMaps.fastIterable(measureToAggregateS))
				.map(entry -> converter.convertCharSequence(entry.getKey(), entry.getValue()));
		Stream<U> streamFromObject = Streams.stream(Object2ObjectMaps.fastIterable(measureToAggregateO))
				.map(entry -> converter.convertObject(entry.getKey(), entry.getValue()));

		return Stream.of(streamFromLong, streamFromDouble, streamFromCharsequence, streamFromObject)
				.flatMap(Functions.identity());
	}

	public long size() {
		long size = 0;

		// Can sum sizes as a key can not appear in multiple columns
		size += measureToAggregateL.size();
		size += measureToAggregateD.size();
		size += measureToAggregateS.size();
		size += measureToAggregateO.size();

		return size;
	}

	public void merge(T key, Object v) {
		// BEWARE This must not assumes doubles necessarily aggregates into a double, longs into a long, etc
		// It is for instance not true in SumElseSetAggregator which turns input String into a collecting Set
		// onValue(key, new ValueConsumer() {
		//
		// @Override
		// public void onLong(long l) {
		//
		// }
		// @Override
		// public void onDouble(double d) {
		//
		// }
		//
		// @Override
		// public void onCharsequence(CharSequence charSequence) {
		//
		// }
		//
		// @Override
		// public void onObject(Object object) {
		//
		// }
		// });
		onValue(key, AsObjectValueConsumer.consumer(existingAggregate -> {
			Object newAggregate = aggregation.aggregate(existingAggregate, v);

			if (existingAggregate != null) {
				clearKey(key);
			}
			put(key, newAggregate);
		}));

		// Aggregate received longs together
		// if (SumAggregator.isLongLike(v)) {
		// long vAsPrimitive = SumAggregator.asLong(v);
		//
		// mergeLong(key, vAsPrimitive);
		// }
		// // Aggregate received doubles together
		// else if (SumAggregator.isDoubleLike(v)) {
		// double vAsPrimitive = SumAggregator.asDouble(v);
		//
		// mergeDouble(key, vAsPrimitive);
		// }
		// // Aggregate received objects together
		//
		// else if (v instanceof CharSequence vAsCharSequence) {
		// mergeCharSequence(key, vAsCharSequence);
		// } else {
		// mergeObject(key, v);
		// }
	}

	// private void mergeObject(T key, Object v) {
	// Object valueToStore;
	//
	// if (measureToAggregateO.containsKey(key)) {
	// Object aggregatedV = aggregation.aggregate(measureToAggregateO.get(key), v);
	// // Replace the existing long by aggregated long
	// valueToStore = aggregatedV;
	// } else {
	// // This is the first encountered String
	// valueToStore = v;
	// }
	//
	// put(key, valueToStore);
	// }
	//
	// protected void mergeCharSequence(T key, CharSequence vAsCharSequence) {
	// CharSequence valueToStore;
	//
	// if (measureToAggregateS.containsKey(key)) {
	// CharSequence aggregatedV = aggregation.aggregateStrings(measureToAggregateS.get(key), vAsCharSequence);
	// // Replace the existing long by aggregated long
	// valueToStore = aggregatedV;
	// } else {
	// // This is the first encountered String
	// valueToStore = vAsCharSequence;
	// }
	//
	// put(key, valueToStore);
	// }
	//
	// protected void mergeDouble(T key, double vAsPrimitive) {
	// double valueToStore;
	// if (measureToAggregateD.containsKey(key)) {
	// double currentV = measureToAggregateD.getDouble(key);
	// // BEWARE What if longs are not aggregated as long?
	// double aggregatedV = aggregation.aggregateDoubles(currentV, vAsPrimitive);
	//
	// // Replace the existing long by aggregated long
	// valueToStore = aggregatedV;
	// } else {
	// // This is the first encountered long
	// valueToStore = vAsPrimitive;
	// }
	// put(key, valueToStore);
	// }
	//
	// protected void mergeLong(T key, long vAsPrimitive) {
	// long valueToStore;
	// if (measureToAggregateL.containsKey(key)) {
	// long currentV = measureToAggregateL.getLong(key);
	// // BEWARE What if longs are not aggregated as long?
	// long aggregatedV = aggregation.aggregateLongs(currentV, vAsPrimitive);
	//
	// // Replace the existing long by aggregated long
	// valueToStore = aggregatedV;
	// } else {
	// // This is the first encountered long
	// valueToStore = vAsPrimitive;
	// }
	// put(key, valueToStore);
	// }

	public Stream<T> keySetStream() {
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
		keySetStream().limit(5).forEach(key -> {

			onValue(key, AsObjectValueConsumer.consumer(o -> {
				toStringHelper.add("#" + index.getAndIncrement(), PepperLogHelper.getObjectAndClass(o));
			}));
		});

		return toStringHelper.toString();
	}

	/**
	 * @return an empty and immutable MultiTypeStorage
	 */
	public static <T> MultiTypeStorage<T> empty() {
		return MultiTypeStorage.<T>builder()
				.measureToAggregateD(Object2DoubleMaps.emptyMap())
				.measureToAggregateL(Object2LongMaps.emptyMap())
				.build();
	}
}
