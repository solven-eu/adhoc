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
package eu.solven.adhoc.measure.sum;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * This is used by {@link IAggregation} which need to differentiate clearly from inputs and a stateful-but-intermediate
 * aggregation.
 * 
 * @author Benoit Lacelle
 */
public interface IAggregationCarrier {

	/**
	 * Typically extended by an {@link IAggregation} requiring an {@link IAggregationCarrier}.
	 * 
	 * @author Benoit Lacelle
	 */
	interface IHasCarriers {

		/**
		 * 
		 * @param v
		 *            some pre-aggregated value, typically computed by the {@link ITableWrapper}. Typically, for
		 *            `RankAggregation`, we want to wrap all input values, not a pre-ranked value.
		 * @return an {@link IAggregationCarrier}
		 */
		@Deprecated(since = "Prefer `IValueReceiver wrap(IValueReceiver sink)`")
		IAggregationCarrier wrap(Object v);

		/**
		 * Used to intercept raw value from {@link ITableWrapper}, to wrap them.
		 * 
		 * @param sink
		 *            the actual receiver
		 * @return the {@link IValueReceiver} to receive the value from {@link ITableWrapper}
		 */
		default IValueReceiver wrap(IValueReceiver sink) {
			return v -> {
				Object wrapped;
				if (v == null) {
					wrapped = null;
				} else {
					// Wrap the aggregate from table into the aggregation custom wrapper
					wrapped = wrap(v);
				}

				sink.onObject(wrapped);
			};
		}

	}

	/**
	 * Enables to read the underlying value of this carrier.
	 * 
	 * @param valueReceiver
	 */
	void acceptValueReceiver(IValueReceiver valueReceiver);
}
