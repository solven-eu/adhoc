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
package eu.solven.adhoc.dataframe.column;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.IColumnValueConverter;
import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.cuboid.slice.Slice;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.query.cube.IHasGroupBy;
import eu.solven.adhoc.query.groupby.GroupByHelpers;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;

/**
 * This is a simple way to store the value for a {@link java.util.Set} of {@link Slice}.
 * 
 * @author Benoit Lacelle
 */
// BEWARE What is the point of this given IMultitypeColumnFastGet? It forces the generic with SliceAsMap. And hides some
// methods/processes like `.purgeAggregationCarriers()`. This is also immutable (by interface).
@ToString
@Builder(toBuilder = true)
public class Cuboid implements ICuboid {
	@NonNull
	// Getter for testing
	@Getter
	final IMultitypeColumnFastGet<ISlice> values;

	@NonNull
	@Singular
	@Getter
	final ImmutableSet<String> columns;

	public static Cuboid empty() {
		return Cuboid.builder().values(MultitypeHashColumn.empty()).build();
	}

	@Override
	public IValueProvider onValue(ISlice slice) {
		return values.onValue(slice);
	}

	@Override
	public Stream<ISlice> slices() {
		return values.keyStream();
	}

	@Override
	public Set<ISlice> slicesSet() {
		return values.keyStream().collect(ImmutableSet.toImmutableSet());
	}

	@Override
	public void forEachSlice(IColumnScanner<ISlice> rowScanner) {
		values.scan(rowScanner);
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<ISlice, U> rowScanner) {
		return values.stream(rowScanner);
	}

	@Override
	public Stream<SliceAndMeasure<ISlice>> stream() {
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
	public Stream<SliceAndMeasure<ISlice>> stream(StreamStrategy strategy) {
		return values.stream(strategy);
	}

	@Override
	public ICuboid purgeCarriers() {
		return Cuboid.builder().values(values.purgeAggregationCarriers()).columns(columns).build();
	}

	@Override
	public void compact() {
		if (values instanceof ICompactable compactable) {
			compactable.compact();
		}
	}

	public static CuboidBuilder forGroupBy(IHasGroupBy hasGroupBy) {
		return Cuboid.builder().columns(hasGroupBy.getGroupBy().getSortedColumns());
	}

	@Override
	public ICuboid mask(Map<String, ?> mask) {
		if (mask.isEmpty()) {
			return this;
		} else if (!Sets.intersection(columns, mask.keySet()).isEmpty()) {
			throw new IllegalArgumentException("Intersection between %s and %s".formatted(columns, mask.keySet()));
		}

		IMultitypeColumnFastGet<ISlice> maskedColumn = GroupByHelpers.addConstantColumns(values, mask);
		return toBuilder().values(maskedColumn).build();
	}

}
