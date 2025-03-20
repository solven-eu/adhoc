/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.data.cell;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * Default implementation of {@link IMultitypeCell}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class MultitypeCell implements IMultitypeCell {
	@NonNull
	IAggregation aggregation;

	@Default
	byte types = 0;

	// `0` is the natural default to most IAggregation
	@Default
	long asLong = 0;
	// `0` is the natural default to most IAggregation
	@Default
	double asDouble = 0D;
	@Default
	CharSequence asCharsequence = null;
	@Default
	Object asObject = null;

	@Override
	public IValueReceiver merge() {
		return new IValueReceiver() {

			@Override
			public void onLong(long v) {
				if (aggregation instanceof ILongAggregation longAggregation) {
					types |= 1;
					MultitypeCell.this.asLong = longAggregation.aggregateLongs(asLong, v);
				} else {
					onObject(v);
				}
			}

			@Override
			public void onDouble(double v) {
				if (aggregation instanceof IDoubleAggregation doubleAggregation) {
					types |= 2;
					MultitypeCell.this.asDouble = doubleAggregation.aggregateDoubles(asDouble, v);
				} else {
					onObject(v);
				}
			}

			@Override
			public void onObject(Object object) {
				types |= 8;
				MultitypeCell.this.asObject = aggregation.aggregate(asObject, object);
			}
		};
	}

	@Override
	public IValueProvider reduce() {
		return vc -> {
			if (types == 1) {
				// Only longs
				vc.onLong(asLong);
			} else if (types == 2) {
				// Only doubles
				vc.onDouble(asDouble);
			} else {
				// Object
				Object crossTypeAggregate = asObject;

				// TODO Would be prefer merging long and double first?
				if ((types & 1) == 1) {
					// There is a long fragment
					crossTypeAggregate = aggregation.aggregate(crossTypeAggregate, asLong);
				}
				if ((types & 2) == 2) {
					// There is a double fragment
					crossTypeAggregate = aggregation.aggregate(crossTypeAggregate, asDouble);
				}

				vc.onObject(crossTypeAggregate);
			}
		};
	}
}
