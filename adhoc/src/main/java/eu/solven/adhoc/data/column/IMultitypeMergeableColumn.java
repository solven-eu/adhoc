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
package eu.solven.adhoc.data.column;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Partitionor;

/**
 * Some {@link IMultitypeColumn} needs no only `.append` but also to `.merge` into an already present slice.
 * 
 * Typically used by {@link Partitionor}.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
public interface IMultitypeMergeableColumn<T> extends IMultitypeColumnFastGet<T> {
	IAggregation getAggregation();

	@Deprecated(since = "Should rely on `IValueConsumer merge(T slice)`")
	default void merge(T slice, Object v) {
		merge(slice).onObject(v);
	}

	/**
	 * Either the slice is missing, and this is similar to a `.append`, or the input value will be aggregated in the
	 * already present aggregate.
	 * 
	 * The aggregation is defined at column instantiation.
	 * 
	 * @param slice
	 */
	IValueReceiver merge(T slice);

}
