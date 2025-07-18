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

import eu.solven.adhoc.data.column.IMultitypeConstants;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.IDoubleAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * Default implementation of {@link IMultitypeCell}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class MultitypeCell implements IMultitypeCell, IValueReceiver, IValueProvider {
	@NonNull
	IAggregation aggregation;

	@Default
	byte types = IMultitypeConstants.MASK_EMPTY;

	// TODO Should we init the default values based on the IAggregation (e.g. ILongAggregation.neutralLong())
	// `0` is the natural default to most IAggregation
	@Default
	long asLong = 0;
	// `0` is the natural default to most IAggregation
	@Default
	double asDouble = 0D;
	@Default
	Object asObject = null;

	@Override
	public void onLong(long v) {
		if (aggregation instanceof ILongAggregation longAggregation) {
			types |= IMultitypeConstants.MASK_LONG;
			asLong = longAggregation.aggregateLongs(asLong, v);
		} else {
			onObject(v);
		}
	}

	@Override
	public void onDouble(double v) {
		if (aggregation instanceof IDoubleAggregation doubleAggregation) {
			types |= IMultitypeConstants.MASK_DOUBLE;
			asDouble = doubleAggregation.aggregateDoubles(asDouble, v);
		} else {
			onObject(v);
		}
	}

	@Override
	public void onObject(Object object) {
		if (object != null) {
			types |= IMultitypeConstants.MASK_OBJECT;
			asObject = aggregation.aggregate(asObject, object);
		}
	}

	@Override
	public IValueReceiver merge() {
		return this;
	}

	@Override
	public void acceptReceiver(IValueReceiver valueReceiver) {
		if (types == IMultitypeConstants.MASK_LONG) {
			// Only longs
			valueReceiver.onLong(asLong);
		} else if (types == IMultitypeConstants.MASK_DOUBLE) {
			// Only doubles
			valueReceiver.onDouble(asDouble);
		} else if (types == IMultitypeConstants.MASK_EMPTY) {
			// empty
			valueReceiver.onObject(null);
		} else {
			// Object
			Object crossTypeAggregate = asObject;

			// TODO Would be prefer merging long and double first?
			if ((types & IMultitypeConstants.MASK_LONG) == IMultitypeConstants.MASK_LONG) {
				// There is a long fragment
				crossTypeAggregate = aggregation.aggregate(crossTypeAggregate, asLong);
			}
			if ((types & IMultitypeConstants.MASK_DOUBLE) == IMultitypeConstants.MASK_DOUBLE) {
				// There is a double fragment
				crossTypeAggregate = aggregation.aggregate(crossTypeAggregate, asDouble);
			}

			valueReceiver.onObject(crossTypeAggregate);
		}
	}

	@Override
	public IValueProvider reduce() {
		return this;
	}

	@SuppressWarnings("PMD.NullAssignment")
	public void clear() {
		types = IMultitypeConstants.MASK_EMPTY;

		asLong = 0;
		asDouble = 0D;
		asObject = null;
	}

	public int getType() {
		return types;
	}
}
