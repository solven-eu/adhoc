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

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;

/**
 * A {@link ISliceToValue} is an immutable data-structure, expressing the mapping from slices to values, typically
 * computed by a {@link ITransformatorQueryStep}.
 * 
 * @author Benoit Lacelle
 */
public interface ISliceToValue extends ICompactable {
	/**
	 * 
	 * @return true if `keySetStream` is already sorted
	 */
	@Deprecated(since = "Some structures can be mixed (e.g. a section is navigable, another is hash)")
	boolean isSorted();

	long size();

	boolean isEmpty();

	Set<String> getColumns();

	Stream<IAdhocSlice> slices();

	Set<IAdhocSlice> slicesSet();

	IValueProvider onValue(IAdhocSlice slice);

	void forEachSlice(IColumnScanner<IAdhocSlice> columnScanner);

	/**
	 * 
	 * @param <U>
	 * @param rowConverter
	 *            knows how to convert a {@link SliceAsMap} and a value through a {@link IValueReceiver} into a custom
	 *            object
	 * @return a {@link Stream} of objects built by the rowConverter
	 */
	@Deprecated(since = "It seems useless", forRemoval = true)
	<U> Stream<U> stream(IColumnValueConverter<IAdhocSlice, U> rowConverter);

	Stream<SliceAndMeasure<IAdhocSlice>> stream();

	/**
	 * 
	 * @param strategy
	 * @return a {@link Stream} with the requested strategy
	 */
	Stream<SliceAndMeasure<IAdhocSlice>> stream(StreamStrategy strategy);

	/**
	 * 
	 * @param storage
	 * @param slice
	 * @return the value as {@link Object} on given slice
	 */
	static Object getValue(ISliceToValue storage, IAdhocSlice slice) {
		return IValueProvider.getValue(storage.onValue(slice));
	}

	/**
	 * 
	 * @return another {@link ISliceToValue} which has been purged from {@link IAggregationCarrier}.
	 */
	ISliceToValue purgeCarriers();

}
