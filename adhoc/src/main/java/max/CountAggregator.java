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
package max;

import eu.solven.adhoc.aggregations.IAggregation;
import lombok.Builder;
import lombok.Value;

/**
 * Keep the highest value amongst encountered values
 */
public class CountAggregator implements IAggregation {

	public static final String KEY = "COUNT";

	// A marker for the special columnName `*`, typically used in `COUNT(*)`
	public static final String ASTERISK = "*";

	@Value
	@Builder
	public static class CountHolder {
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
	}

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return r;
		} else if (r == null) {
			return l;
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

	@Override
	public double aggregateDoubles(double left, double right) {
		throw new UnsupportedOperationException("COUNT does not fit into a double");
	}

	@Override
	public long aggregateLongs(long left, long right) {
		throw new UnsupportedOperationException("COUNT does not fit into a double");
	}
}
