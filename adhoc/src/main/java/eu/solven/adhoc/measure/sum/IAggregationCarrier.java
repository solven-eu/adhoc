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

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.storage.IValueConsumer;
import eu.solven.adhoc.table.IAdhocTableWrapper;

/**
 * This is used by {@link IAggregation} which need to differentiate clearly from inputs and a stateful-but-intermediate
 * aggregation.
 * 
 * @author Benoit Lacelle
 */
public interface IAggregationCarrier {

	interface IHasCarriers {

		/**
		 * 
		 * @param v
		 *            some pre-aggregated value, typically computed by the {@link IAdhocTableWrapper}.
		 * @return an {@link IAggregationCarrier}
		 */
		IAggregationCarrier wrap(Object v);

	}

	/**
	 * Enables to read the underlying value of this carrier.
	 * 
	 * @param valueConsumer
	 */
	void acceptValueConsumer(IValueConsumer valueConsumer);
}
