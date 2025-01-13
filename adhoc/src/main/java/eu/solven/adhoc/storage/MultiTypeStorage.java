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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import eu.solven.adhoc.RowScanner;
import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongMaps;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * This data-structures aggregates input value on a per-key basis. Different keys are allowed to be associated to
 * different types (e.g. we may have some keys holding a functional double, while other keys may hold an error String).
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
	// We do not try aggregating same type together, for a final cross-type aggregation. This could be done in a later
	// implementation
	// but with unclear benefits.
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
	 * A put operation
	 * 
	 * @param key
	 * @param v
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
		} else {
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

	public void onValue(T key, ValueConsumer consumer) {
		if (measureToAggregateL.containsKey(key)) {
			consumer.onLong(measureToAggregateL.getLong(key));
		} else if (measureToAggregateD.containsKey(key)) {
			consumer.onDouble(measureToAggregateD.getDouble(key));
		} else if (measureToAggregateS.containsKey(key)) {
			consumer.onCharsequence(measureToAggregateS.get(key));
		} else {
			consumer.onObject(measureToAggregateO.get(key));
		}
	}

	public void scan(RowScanner<T> rowScanner) {
		keySet().forEach(key -> {
			onValue(key, rowScanner.onKey(key));
		});
	}

	public long size() {
		long size = 0;

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

			clearKey(key);
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

	public Set<T> keySet() {
		Set<T> keySet = new HashSet<>();

		keySet.addAll(measureToAggregateD.keySet());
		keySet.addAll(measureToAggregateL.keySet());
		keySet.addAll(measureToAggregateS.keySet());
		keySet.addAll(measureToAggregateO.keySet());

		return keySet;
	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this)
				.add("measureToAggregateD.size()", measureToAggregateD.size())
				.add("measureToAggregateL.size()", measureToAggregateL.size())
				.add("measureToAggregateS.size()", measureToAggregateS.size())
				.add("measureToAggregateO.size()", measureToAggregateO.size());

		AtomicInteger index = new AtomicInteger();
		keySet().stream().limit(5).forEach(key -> {

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
