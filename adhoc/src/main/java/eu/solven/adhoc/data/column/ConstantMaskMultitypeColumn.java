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

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Decorate a {@link IMultitypeColumnFastGet} to that each slice behave like having an additional {@link Set} of
 * columns.
 * 
 * The mask must not overlap columns in the masked {@link IMultitypeColumnFastGet}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class ConstantMaskMultitypeColumn implements IMultitypeColumnFastGet<SliceAsMap> {

	@NonNull
	final IMultitypeColumnFastGet<SliceAsMap> masked;
	@NonNull
	final ImmutableMap<String, ?> mask;

	@Override
	public long size() {
		return masked.size();
	}

	@Override
	public boolean isEmpty() {
		return masked.isEmpty();
	}

	@Override
	public void purgeAggregationCarriers() {
		masked.purgeAggregationCarriers();
	}

	@Override
	public void scan(IColumnScanner<SliceAsMap> rowScanner) {
		masked.scan(unmaskedSlice -> {
			return rowScanner.onKey(extendSlice(unmaskedSlice));
		});
	}

	protected SliceAsMap extendSlice(SliceAsMap unmaskedSlice) {
		return unmaskedSlice.addColumns(mask);
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<SliceAsMap, U> converter) {
		throw new NotYetImplementedException("TODO");
	}

	@Override
	public Stream<SliceAndMeasure<SliceAsMap>> stream() {
		throw new NotYetImplementedException("TODO");
	}

	@Override
	public IValueProvider onValue(SliceAsMap key) {
		if (!key.getAdhocSliceAsMap().getCoordinates().entrySet().containsAll(mask.entrySet())) {
			// This is not a compatible key
			return vc -> vc.onObject(null);
		}

		throw new NotYetImplementedException("TODO");
	}

	@Override
	public Stream<SliceAsMap> keyStream() {
		return masked.keyStream().map(this::extendSlice);
	}

	@Override
	public IValueReceiver append(SliceAsMap slice) {
		throw new UnsupportedOperationException("%s is immutable".formatted(this.getClass().getName()));
	}

	@Override
	public IValueReceiver set(SliceAsMap key) {
		throw new UnsupportedOperationException("%s is immutable".formatted(this.getClass().getName()));
	}

}
