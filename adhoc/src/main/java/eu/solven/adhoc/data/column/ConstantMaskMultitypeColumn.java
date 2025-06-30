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

import java.util.LinkedHashMap;
import java.util.Map;
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
import lombok.Singular;
import lombok.Value;

/**
 * Decorate a {@link IMultitypeColumnFastGet} to that each slice behave like having an additional {@link Set} of
 * columns.
 * 
 * The mask must not overlap columns in the masked {@link IMultitypeColumnFastGet}. Such a constraint may be detected
 * lazily, but generally not when creating this.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class ConstantMaskMultitypeColumn implements IMultitypeColumnFastGet<SliceAsMap> {

	@NonNull
	final IMultitypeColumnFastGet<SliceAsMap> masked;
	@NonNull
	@Singular
	final ImmutableMap<String, ?> masks;

	@Override
	public long size() {
		return masked.size();
	}

	@Override
	public boolean isEmpty() {
		return masked.isEmpty();
	}

	@Override
	public IMultitypeColumnFastGet<SliceAsMap> purgeAggregationCarriers() {
		return ConstantMaskMultitypeColumn.builder().masked(masked.purgeAggregationCarriers()).masks(masks).build();
	}

	@Override
	public void scan(IColumnScanner<SliceAsMap> rowScanner) {
		masked.scan(unmaskedSlice -> rowScanner.onKey(extendSlice(unmaskedSlice)));
	}

	protected SliceAsMap extendSlice(SliceAsMap unmaskedSlice) {
		return unmaskedSlice.addColumns(masks);
	}

	protected SliceAsMap getUnmaskedSlice(Map<String, ?> keyCoordinates) {
		// BEWARE This assumes the underlying IMultitypeColumnFastGet does not express columns in the mask
		Map<String, Object> unmaskedKey = new LinkedHashMap<>(keyCoordinates);
		unmaskedKey.keySet().removeAll(masks.keySet());

		SliceAsMap unmaskedSlice = SliceAsMap.fromMap(unmaskedKey);
		return unmaskedSlice;
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<SliceAsMap, U> converter) {
		throw new NotYetImplementedException("TODO");
	}

	@Override
	public Stream<SliceAndMeasure<SliceAsMap>> stream() {
		return masked.stream().map(maskedSliceAndMeasure -> {
			return SliceAndMeasure.<SliceAsMap>builder()
					.slice(extendSlice(maskedSliceAndMeasure.getSlice()))
					.valueProvider(maskedSliceAndMeasure.getValueProvider())
					.build();
		});
	}

	@Override
	public IValueProvider onValue(SliceAsMap key) {
		Map<String, Object> keyCoordinates = key.getAdhocSliceAsMap().getCoordinates();
		if (!keyCoordinates.entrySet().containsAll(masks.entrySet())) {
			// This is not a compatible key
			return vc -> vc.onObject(null);
		}

		SliceAsMap unmaskedSlice = getUnmaskedSlice(keyCoordinates);
		return masked.onValue(unmaskedSlice);
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
