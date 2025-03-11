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
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.storage.IValueProvider;
import eu.solven.adhoc.storage.IValueReceiver;
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

	@NonNull
	IAggregation aggregation;

	@Override
	public IValueReceiver merge(T key) {

		return new IValueReceiver() {
			@Override
			public void onLong(long v) {
				onValue(key, new IValueReceiver() {
					@Override
					public void onLong(long existingAggregate) {
						if (aggregation instanceof ILongAggregation longAggregation) {
							long newAggregate = longAggregation.aggregateLongs(existingAggregate, v);

							// No need to clear as we replace a long with a long
							unsafePut(key, false).onLong(newAggregate);
						} else {
							Object newAggregate = aggregation.aggregate(existingAggregate, v);

							if (SumAggregation.isLongLike(newAggregate)) {
								long newAggregateAsLong = SumAggregation.asLong(newAggregate);
								unsafePut(key, false).onLong(newAggregateAsLong);
							} else {
								// Clear long
								measureToAggregateL.removeLong(key);
								unsafePut(key, false).onObject(newAggregate);
							}
						}
					}

					@Override
					public void onObject(Object existingAggregate) {
						Object newAggregate = aggregation.aggregate(existingAggregate, v);

						boolean clearKey = existingAggregate != null;
						unsafePut(key, clearKey).onObject(newAggregate);
					}
				});
			}

			@Override
			public void onDouble(double v) {
				onValue(key, new IValueReceiver() {
					@Override
					public void onDouble(double existingAggregate) {
						if (aggregation instanceof IDoubleAggregation doubleAggregation) {
							double newAggregate = doubleAggregation.aggregateDoubles(existingAggregate, v);

							// No need to clear as we replace a long with a long
							unsafePut(key, false).onDouble(newAggregate);
						} else {
							Object newAggregate = aggregation.aggregate(existingAggregate, v);

							if (SumAggregation.isDoubleLike(newAggregate)) {
								double newAggregateAsDouble = SumAggregation.asDouble(newAggregate);
								unsafePut(key, false).onDouble(newAggregateAsDouble);
							} else {
								// Clear double
								measureToAggregateD.removeDouble(key);
								unsafePut(key, false).onObject(newAggregate);
							}
						}
					}

					@Override
					public void onObject(Object existingAggregate) {
						Object newAggregate = aggregation.aggregate(existingAggregate, v);

						boolean clearKey = existingAggregate != null;
						unsafePut(key, clearKey).onObject(newAggregate);
					}
				});
			}

			@Override
			public void onObject(Object v) {
				IValueProvider existingAggregate = onValue(key);
				aggregation.aggregate(existingAggregate, vc -> vc.onObject(v)).acceptConsumer(set(key));
			}
		};
	}
}
