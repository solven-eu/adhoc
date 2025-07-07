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
package eu.solven.adhoc.data.tabular;

import java.util.stream.Stream;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IColumnValueConverter;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import lombok.Builder;

/**
 * Undictionarize a {@link IMultitypeColumnFastGet}, based on an Integer key, and `int-from/to-Object` mappings.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
@Builder
public class UndictionarizedColumn<T> implements IMultitypeColumnFastGet<T> {
	private final Int2ObjectFunction<T> indexToSlice;
	private final Object2IntFunction<T> sliceToIndex;
	private final IMultitypeColumnFastGet<Integer> column;

	@Override
	public long size() {
		return column.size();
	}

	@Override
	public boolean isEmpty() {
		return column.isEmpty();
	}

	@Override
	public IMultitypeColumnFastGet<T> purgeAggregationCarriers() {
		return UndictionarizedColumn.<T>builder()
				.indexToSlice(indexToSlice)
				.sliceToIndex(sliceToIndex)
				.column(column.purgeAggregationCarriers())
				.build();
	}

	@Override
	public IValueReceiver append(T slice) {
		throw new UnsupportedOperationException("Read-Only");
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		column.scan(rowIndex -> rowScanner.onKey(indexToSlice.apply(rowIndex)));
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		return column.stream(rowIndex -> converter.prepare(indexToSlice.apply(rowIndex)));
	}

	@Override
	public Stream<SliceAndMeasure<T>> stream() {
		return column.stream()
				.map(sliceAndMeasure -> SliceAndMeasure.<T>builder()
						.slice(indexToSlice.get(sliceAndMeasure.getSlice()))
						.valueProvider(sliceAndMeasure.getValueProvider())
						.build());
	}

	@Override
	public Stream<T> keyStream() {
		return column.keyStream().map(indexToSlice::apply);
	}

	@Override
	public IValueProvider onValue(T key) {
		return column.onValue(sliceToIndex.apply(key));
	}

	@Override
	public IValueReceiver set(T key) {
		throw new UnsupportedOperationException("Read-Only");
	}

	@Override
	public String toString() {
		return "column=" + column;
	}

	// @Override
	// public void ensureCapacity(int capacity) {
	// throw new UnsupportedOperationException("Read-Only");
	// }
}