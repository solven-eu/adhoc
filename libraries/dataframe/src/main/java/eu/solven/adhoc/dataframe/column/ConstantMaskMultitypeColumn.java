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
package eu.solven.adhoc.dataframe.column;

import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.IColumnValueConverter;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.options.IHasOptionsAndExecutorService;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.stream.IConsumingStream;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Decorate a {@link IMultitypeColumnFastGet} so that each slice behaves like having an additional {@link Set} of
 * columns.
 * 
 * The mask must not overlap columns in the masked {@link IMultitypeColumnFastGet}. Such a constraint may be detected
 * lazily, but generally not when creating this.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class ConstantMaskMultitypeColumn implements IMultitypeColumnFastGet<ISlice> {

	@NonNull
	@Default
	ISliceFactory factory = AdhocFactoriesUnsafe.factories.getSliceFactoryFactory()
			.makeFactory(IHasOptionsAndExecutorService.noOption());

	@NonNull
	final IMultitypeColumnFastGet<ISlice> masked;
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
	public IMultitypeColumnFastGet<ISlice> purgeAggregationCarriers() {
		return ConstantMaskMultitypeColumn.builder().masked(masked.purgeAggregationCarriers()).masks(masks).build();
	}

	@Override
	public void scan(IColumnScanner<ISlice> rowScanner) {
		masked.scan(unmaskedSlice -> rowScanner.onKey(extendSlice(unmaskedSlice)));
	}

	protected ISlice extendSlice(ISlice unmaskedSlice) {
		return unmaskedSlice.addColumns(masks);
	}

	protected ISlice getUnmaskedSlice(IAdhocMap key) {
		// TODO Should we have registered the set of masked columns at construction?
		// BEWARE This assumes the underlying IMultitypeColumnFastGet does not express columns in the mask
		return key.retainAll(Sets.difference(key.keySet(), masks.keySet())).asSlice();
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<ISlice, U> converter) {
		throw new NotYetImplementedException("TODO");
	}

	@Override
	public IConsumingStream<SliceAndMeasure<ISlice>> stream() {
		return masked.stream().map(maskedSliceAndMeasure -> {
			return SliceAndMeasure.<ISlice>builder()
					.slice(extendSlice(maskedSliceAndMeasure.getSlice()))
					.valueProvider(maskedSliceAndMeasure.getValueProvider())
					.build();
		});
	}

	@Override
	public IValueProvider onValue(ISlice key) {
		IAdhocMap keyAsMap = key.asAdhocMap();
		if (!keyAsMap.entrySet().containsAll(masks.entrySet())) {
			// This is not a compatible key
			return vc -> vc.onObject(null);
		}

		ISlice unmaskedSlice = getUnmaskedSlice(keyAsMap);
		return masked.onValue(unmaskedSlice);
	}

	@Override
	public IConsumingStream<ISlice> keyStream() {
		return masked.keyStream().map(this::extendSlice);
	}

	@Override
	public IValueReceiver append(ISlice slice) {
		throw new UnsupportedOperationException("%s is immutable".formatted(this.getClass().getName()));
	}

}
