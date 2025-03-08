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
package eu.solven.adhoc.storage;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.slice.SliceAsMap;
import eu.solven.adhoc.storage.column.IColumnScanner;
import eu.solven.adhoc.storage.column.IColumnValueConverter;
import eu.solven.adhoc.storage.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.storage.column.MultitypeHashColumn;
import eu.solven.adhoc.storage.column.MultitypeNavigableColumn;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;

/**
 * This is a simple way to storage the value for a {@link java.util.Set} of {@link SliceAsMap}.
 */
// BEWARE What is the point of this given IMultitypeColumnFastGet? It forces the generic with SliceAsMap. And hides some
// methods/processes like `.purgeAggregationCarriers()`. This is also immutable.
@Builder
@ToString
public class SliceToValue implements ISliceToValue {
	@NonNull
	// @Default
	final IMultitypeColumnFastGet<SliceAsMap> column
	// = MultiTypeStorageHash.<SliceAsMap>builder().build()
	;

	public static SliceToValue empty() {
		return SliceToValue.builder().column(MultitypeHashColumn.empty()).build();
	}

	@Override
	public void onValue(SliceAsMap slice, IValueConsumer consumer) {
		column.onValue(slice.getAdhocSliceAsMap(), consumer);
	}

	@Override
	public Stream<SliceAsMap> keySetStream() {
		return column.keyStream();
	}

	@Override
	public Set<SliceAsMap> slicesSet() {
		return column.keyStream().collect(Collectors.toSet());
	}

	@Override
	public void forEachSlice(IColumnScanner<SliceAsMap> rowScanner) {
		column.scan(rowScanner);
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<SliceAsMap, U> rowScanner) {
		return column.stream(rowScanner);
	}

	@Override
	public Stream<SliceAndMeasure<SliceAsMap>> stream() {
		return column.stream();
	}

	@Override
	public long size() {
		return column.size();
	}

	@Override
	public boolean isEmpty() {
		return column.isEmpty();
	}

	@Override
	public boolean isSorted() {
		if (column instanceof MultitypeNavigableColumn<SliceAsMap>) {
			// TODO Introduce dedicated interface
			// .keySetStream().spliterator().hasCharacteristics(Spliterator.SORTED)
			return true;
		}
		return false;
	}

}
