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
package eu.solven.adhoc.data.column;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.query.cube.IHasGroupBy;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;

/**
 * This is a simple way to storage the value for a {@link java.util.Set} of {@link SliceAsMap}.
 * 
 * @author Benoit Lacelle
 */
// BEWARE What is the point of this given IMultitypeColumnFastGet? It forces the generic with SliceAsMap. And hides some
// methods/processes like `.purgeAggregationCarriers()`. This is also immutable (by interface).
@ToString
@Builder
public class SliceToValue implements ISliceToValue {
	@NonNull
	// Getter for testing
	@Getter
	final IMultitypeColumnFastGet<SliceAsMap> values;

	@NonNull
	@Singular
	@Getter
	final ImmutableSet<String> columns;

	public static SliceToValue empty() {
		return SliceToValue.builder().values(MultitypeHashColumn.empty()).build();
	}

	@Override
	public IValueProvider onValue(SliceAsMap slice) {
		return values.onValue(slice);
	}

	@Override
	public Stream<SliceAsMap> slices() {
		return values.keyStream();
	}

	@Override
	public Set<SliceAsMap> slicesSet() {
		return values.keyStream().collect(Collectors.toSet());
	}

	@Override
	public void forEachSlice(IColumnScanner<SliceAsMap> rowScanner) {
		values.scan(rowScanner);
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<SliceAsMap, U> rowScanner) {
		return values.stream(rowScanner);
	}

	@Override
	public Stream<SliceAndMeasure<SliceAsMap>> stream() {
		return values.stream();
	}

	@Override
	public long size() {
		return values.size();
	}

	@Override
	public boolean isEmpty() {
		return values.isEmpty();
	}

	@Override
	public boolean isSorted() {
		if (values instanceof IIsSorted) {
			return true;
		}
		return false;
	}

	@Override
	public Stream<SliceAndMeasure<SliceAsMap>> stream(StreamStrategy strategy) {
		return values.stream(strategy);
	}

	@Override
	public ISliceToValue purgeCarriers() {
		return SliceToValue.builder().values(values.purgeAggregationCarriers()).columns(columns).build();
	}

	@Override
	public void compact() {
		if (values instanceof ICompactable compactable) {
			compactable.compact();
		}
	}

	public static SliceToValueBuilder forGroupBy(IHasGroupBy hasGroupBy) {
		return SliceToValue.builder().columns(hasGroupBy.getGroupBy().getGroupedByColumns());
	}

}
