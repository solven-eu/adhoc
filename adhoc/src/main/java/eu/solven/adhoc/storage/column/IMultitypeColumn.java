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
package eu.solven.adhoc.storage.column;

import java.util.Map;
import java.util.stream.Stream;

import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.storage.IValueConsumer;
import eu.solven.adhoc.table.IAdhocTableWrapper;

/**
 * This is similar to a {@link Map}, but it is specialized for full-scan read operations and `.append`. If you need
 * `.merge()` capabilities, consider {@link IMultitypeMergeableColumn}.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
public interface IMultitypeColumn<T> {

	/**
	 * 
	 * @return the number of different keys.
	 */
	long size();

	boolean isEmpty();

	/**
	 * Typically called by {@link AdhocQueryEngine} once an {@link IAdhocTableWrapper} measure is fully received, to
	 * turn {@link IAggregationCarrier} into the real aggregate (e.g. turning a CountCarrier, holding a long, to be
	 * differentiated with a column holding longs).
	 */
	void purgeAggregationCarriers();

	/**
	 * 
	 * @param slice
	 * @return the {@link IValueConsumer} into which the value has to be written.
	 */
	IValueConsumer append(T slice);

	@Deprecated(since = "Should rely on `IValueConsumer append(T slice)`")
	default void append(T slice, Object o) {
		append(slice).onObject(o);
	}

	void scan(IColumnScanner<T> rowScanner);

	<U> Stream<U> stream(IColumnValueConverter<T, U> converter);

	Stream<SliceAndMeasure<T>> stream();

	Stream<T> keyStream();

}
