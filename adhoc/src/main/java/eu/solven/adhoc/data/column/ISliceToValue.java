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

import java.util.Set;
import java.util.stream.Stream;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;

/**
 * A {@link ISliceToValue} is an immutable data-structure, expressing the mapping from slices to values, typically
 * computed by a {@link ITransformatorQueryStep}.
 */
public interface ISliceToValue {
	/**
	 * 
	 * @return true if `keySetStream` is already sorted
	 */
	boolean isSorted();

	long size();

	boolean isEmpty();

	Stream<SliceAsMap> keySetStream();

	Set<SliceAsMap> slicesSet();

	void onValue(SliceAsMap slice, IValueReceiver valueReceiver);

	void forEachSlice(IColumnScanner<SliceAsMap> columnScanner);

	/**
	 * 
	 * @param <U>
	 * @param rowConverter
	 *            knows how to convert a {@link SliceAsMap} and a value through a {@link IValueReceiver} into a custom
	 *            object
	 * @return a {@link Stream} of objects built by the rowConverter
	 */
	<U> Stream<U> stream(IColumnValueConverter<SliceAsMap, U> rowConverter);

	Stream<SliceAndMeasure<SliceAsMap>> stream();

	/**
	 * 
	 * @param <T>
	 * @param storage
	 * @param slice
	 * @return the value as {@link Object} on given slice
	 */
	static <T> Object getValue(ISliceToValue storage, IAdhocSlice slice) {
		SliceAsMap sliceAsMap = slice.getAdhocSliceAsMap();

		IValueProvider valueProvider = valueConsumer -> storage.onValue(sliceAsMap, valueConsumer);

		return IValueProvider.getValue(valueProvider);
	}

}
