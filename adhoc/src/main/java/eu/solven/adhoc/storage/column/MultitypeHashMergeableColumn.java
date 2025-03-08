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

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.storage.IValueConsumer;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * This data-structures aggregates input value on a per-key basis. Different keys are allowed to be associated to
 * different types (e.g. we may have some keys holding a functional double, while other keys may hold an error String).
 * <p>
 * This data-structure does not maintain order. Typically `SUM(123, 'a', 234)` could lead to `'123a234'` or `'357a'`.
 *
 * @param <T>
 */
@SuperBuilder
@Slf4j
public class MultitypeHashMergeableColumn<T> extends MultitypeHashColumn<T> implements IMultitypeMergeableColumn<T> {

	// @Default
	@NonNull
	IAggregation aggregation // = new SumAggregation()
	;

	@Override
	public IValueConsumer merge(T key) {
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

		return v -> {
			onValue(key, existingAggregate -> {
				Object newAggregate = aggregation.aggregate(existingAggregate, v);

				if (existingAggregate != null) {
					clearKey(key);
				}
				append(key, newAggregate);
			});
		};

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

}
