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

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.IAggregationCarrier.IHasCarriers;
import eu.solven.adhoc.storage.IValueConsumer;
import lombok.Builder;
import lombok.Value;

/**
 * Keep the highest value amongst encountered values
 */
public class CountAggregation implements IAggregation, IHasCarriers {

	public static final String KEY = "COUNT";

	/**
	 * This class holds the count. It is useful to differentiate as input long (which count as `1`) and a count.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class CountHolder implements IAggregationCarrier {
		long count;

		public static CountHolder zero() {
			return new CountHolder(0);
		}

		public CountHolder aggregate(Object input) {
			if (input instanceof CountHolder inputCountHolder) {
				return add(inputCountHolder.count);
			} else {
				return increment();
			}
		}

		public CountHolder increment() {
			return new CountHolder(this.count + 1);
		}

		public CountHolder add(long input) {
			return new CountHolder(this.count + input);
		}

		@Override
		public void acceptValueConsumer(IValueConsumer valueConsumer) {
			valueConsumer.onLong(count);
		}
	}

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return aggregateOne(r);
		} else if (r == null) {
			return aggregateOne(l);
		} else {
			if (l instanceof CountHolder countHolder) {
				return countHolder.aggregate(r);
			} else if (r instanceof CountHolder countHolder) {
				return countHolder.aggregate(l);
			} else {
				return CountHolder.zero().aggregate(l).aggregate(r);
			}
		}
	}

	protected Object aggregateOne(Object one) {
		if (one instanceof CountHolder) {
			return one;
		} else {
			return CountHolder.zero().increment();
		}
	}

	@Override
	public CountHolder wrap(Object v) {
		return CountHolder.builder().count(((Number) v).longValue()).build();
	}

	// @Override
	// public double aggregateDoubles(double left, double right) {
	// throw new UnsupportedOperationException("COUNT does not fit into a double");
	// }

	// @Override
	// public long aggregateLongs(long left, long right) {
	// throw new UnsupportedOperationException("COUNT does not fit into a double");
	// }
}
