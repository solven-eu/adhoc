/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.dataframe.column.merge;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;

/**
 * 
 * @author Benoit Lacelle
 */
public final class MergingIntColumnValueReceiver implements IValueReceiver {
	final int key;
	final IAggregation aggregation;
	final IIntColumnPutter putter;
	final IValueProvider existingAggregate;

	public MergingIntColumnValueReceiver(IAggregation aggregation, IIntColumnPutter putter, int key) {
		this.key = key;
		this.aggregation = aggregation;
		this.putter = putter;
		this.existingAggregate = putter.onValue(key);
	}

	@SuppressWarnings("CPD-START")
	@Override
	public void onLong(long v) {
		existingAggregate.acceptReceiver(new IValueReceiver() {
			@Override
			public void onLong(long existingAggregate) {
				if (aggregation instanceof ILongAggregation longAggregation) {
					long newAggregate = longAggregation.aggregateLongs(existingAggregate, v);

					// No need to clear as we replace a long with a long
					putter.unsafePut(key, false).onLong(newAggregate);
				} else {
					Object newAggregate = aggregation.aggregate(existingAggregate, v);

					if (AdhocPrimitiveHelpers.isLongLike(newAggregate)) {
						long newAggregateAsLong = AdhocPrimitiveHelpers.asLong(newAggregate);
						putter.unsafePut(key, false).onLong(newAggregateAsLong);
					} else {
						// Clear long
						putter.removeLong(key);
						putter.unsafePut(key, false).onObject(newAggregate);
					}
				}
			}

			// @Override
			protected void onNull() {
				if (aggregation instanceof ILongAggregation longAggregation) {
					long newAggregate = longAggregation.aggregateLongs(longAggregation.neutralLong(), v);

					// No need to clear as we replace a long with a long
					putter.unsafePut(key, false).onLong(newAggregate);
				} else {
					Object newAggregate = aggregation.aggregate(null, v);

					if (AdhocPrimitiveHelpers.isLongLike(newAggregate)) {
						long newAggregateAsLong = AdhocPrimitiveHelpers.asLong(newAggregate);
						putter.unsafePut(key, false).onLong(newAggregateAsLong);
					} else {
						// Clear long
						putter.removeLong(key);
						putter.unsafePut(key, false).onObject(newAggregate);
					}
				}
			}

			@Override
			public void onObject(Object existingAggregate) {
				if (existingAggregate == null) {
					onNull();
				} else {
					Object newAggregate = aggregation.aggregate(existingAggregate, v);

					boolean clearKey = existingAggregate != null;
					putter.unsafePut(key, clearKey).onObject(newAggregate);
				}
			}
		});
	}

	@Override
	public void onDouble(double v) {
		existingAggregate.acceptReceiver(new IValueReceiver() {
			@Override
			public void onDouble(double existingAggregate) {
				if (aggregation instanceof IDoubleAggregation doubleAggregation) {
					double newAggregate = doubleAggregation.aggregateDoubles(existingAggregate, v);

					// No need to clear as we replace a double with a double
					putter.unsafePut(key, false).onDouble(newAggregate);
				} else {
					Object newAggregate = aggregation.aggregate(existingAggregate, v);

					if (AdhocPrimitiveHelpers.isDoubleLike(newAggregate)) {
						double newAggregateAsDouble = AdhocPrimitiveHelpers.asDouble(newAggregate);
						putter.unsafePut(key, false).onDouble(newAggregateAsDouble);
					} else {
						// Clear double
						putter.removeDouble(key);
						putter.unsafePut(key, false).onObject(newAggregate);
					}
				}
			}

			@Override
			public void onObject(Object existingAggregate) {
				Object newAggregate = aggregation.aggregate(existingAggregate, v);

				boolean clearKey = existingAggregate != null;
				putter.unsafePut(key, clearKey).onObject(newAggregate);
			}
		});
	}

	@Override
	public void onObject(Object v) {
		aggregation.aggregate(existingAggregate, vc -> vc.onObject(v)).acceptReceiver(putter.unsafePut(key, true));
	}
}