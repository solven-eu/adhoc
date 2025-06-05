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
package eu.solven.adhoc.measure.sum;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Value;

/**
 * Count the number of received entries. Typically used for `COUNT(*)` row count.
 * 
 * @author Benoit Lacelle
 */
public class CountAggregation implements IAggregation, IAggregationCarrier.IHasCarriers {

	public static final String KEY = "COUNT";

	private static final CountAggregationCarrier COUNT_1 = CountAggregationCarrier.builder().count(1).build();

	/**
	 * {@link IAggregationCarrier} which holds the actual acount, while a received `int` or `long` may actually means
	 * `count=1`.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	public static class CountAggregationCarrier implements IAggregationCarrier {
		long count;

		@Builder
		public CountAggregationCarrier(long count) {
			if (count <= 0) {
				throw new IllegalArgumentException("COUNT has to be strictly positive");
			}

			this.count = count;
		}

		@Override
		public void acceptValueReceiver(IValueReceiver valueReceiver) {
			valueReceiver.onLong(count);
		}
	}

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return aggregateOne(r);
		} else if (r == null) {
			return aggregateOne(l);
		} else {
			long count = 0;

			if (l instanceof CountAggregationCarrier carrier) {
				count += carrier.getCount();
			} else {
				count += 1;
			}
			if (r instanceof CountAggregationCarrier carrier) {
				count += carrier.getCount();
			} else {
				count += 1;
			}

			return CountAggregationCarrier.builder().count(count).build();
		}
	}

	protected Object aggregateOne(Object one) {
		if (one == null) {
			return null;
		} else if (one instanceof CountAggregationCarrier carrier) {
			return carrier;
		} else {
			return COUNT_1;
		}
	}

	public static boolean isCount(String aggregationKey) {
		return KEY.equals(aggregationKey) || CountAggregation.class.getName().equals(aggregationKey);
	}

	@Override
	public IAggregationCarrier wrap(Object rawCount) {
		if (AdhocPrimitiveHelpers.isLongLike(rawCount)) {
			long asLong = AdhocPrimitiveHelpers.asLong(rawCount);
			if (asLong == 0L) {
				// This may happens on SQL like `count(*) FILTER (c = 'v')`
				return null;
			}
			return CountAggregationCarrier.builder().count(asLong).build();
		} else if (rawCount instanceof CountAggregationCarrier count) {
			return count;
		} else {
			throw new IllegalArgumentException(
					"Unexpected raw count: %s".formatted(PepperLogHelper.getObjectAndClass(rawCount)));
		}
	}
}
