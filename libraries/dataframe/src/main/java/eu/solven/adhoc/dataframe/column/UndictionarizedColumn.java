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

import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.IColumnValueConverter;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.stream.IConsumingStream;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * Undictionarize a {@link IMultitypeColumnFastGet}, based on an Integer key, and `int-from/to-Object` mappings.
 *
 * <p>
 * The wrapped {@code column} is keyed by sequential dictionarization indices (0, 1, 2, …) — its internal "sortedness"
 * is by Integer key, NOT by slice. To recover slice-sorted semantics for downstream consumers, the builder accepts a
 * {@link #sortedLength}: the number of leading indices for which the corresponding slices were inserted in strictly
 * increasing slice order. Indices {@code [0, sortedPrefixLength)} are guaranteed to map to slices in slice-ascending
 * order; indices {@code [sortedPrefixLength, size)} are in dictionarization-insertion order.
 *
 * <p>
 * The {@link StreamStrategy#SORTED_SUB} / {@link StreamStrategy#SORTED_SUB_COMPLEMENT} overrides on
 * {@link #stream(StreamStrategy)} and {@link #onValue(Object, StreamStrategy)} use {@code sortedPrefixLength} to expose
 * only the genuinely-sorted prefix as a sorted leg.
 *
 * @param <T>
 *            the slice key type
 * @author Benoit Lacelle
 */
@Builder
public class UndictionarizedColumn<T> implements IMultitypeColumnFastGet<T>, ICompactable {
	@NonNull
	private final IntFunction<T> indexToSlice;
	@NonNull
	private final ToIntFunction<T> sliceToIndex;
	/**
	 * The dictionarized inner column, always typed through the int-specialized interface so the hot-path
	 * {@link #onValue(Object)} / {@link #onValue(Object, StreamStrategy)} lookups call {@code column.onValue(int)}
	 * directly without paying {@link Integer#valueOf} allocation. Callers whose storage is not natively int-specialized
	 * must adapt through {@link IMultitypeIntColumnFastGet#wrap(IMultitypeColumnFastGet)}; when the inner column
	 * <em>is</em> already int-specialized (e.g. {@code MultitypeNavigableIntColumn} from
	 * {@code AggregatingColumnsDistinct}), the {@code wrap} call returns the same reference with no adapter
	 * indirection.
	 */
	@NonNull
	private final IMultitypeIntColumnFastGet column;

	/**
	 * Number of leading indices for which the wrapped column's slices were inserted in strictly increasing slice order.
	 * Default {@code 0} preserves legacy behavior (no slices considered sorted).
	 */
	@Default
	private final int sortedLength = 0;

	@Default
	private final IntPredicate sortedLeg = i -> false;

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
		// `column` is an IMultitypeIntColumnFastGet; its covariant `purgeAggregationCarriers()` override guarantees
		// the purged copy is still int-specialized, so the builder accepts it without any adaptation.
		return UndictionarizedColumn.<T>builder()
				.indexToSlice(indexToSlice)
				.sliceToIndex(sliceToIndex)
				.column(column.purgeAggregationCarriers())
				.sortedLength(sortedLength)
				.sortedLeg(sortedLeg)
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
	public IConsumingStream<SliceAndMeasure<T>> stream() {
		return column.stream()
				.map(sliceAndMeasure -> SliceAndMeasure.<T>builder()
						.slice(indexToSlice.apply(sliceAndMeasure.getSlice()))
						.valueProvider(sliceAndMeasure.getValueProvider())
						.build());
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> limit(int limit) {
		throw new NotYetImplementedException("Needed?");
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> skip(int skip) {
		throw new NotYetImplementedException("Needed?");
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> stream(StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL -> stream();
		// The inner column is keyed by dictionarization index and (post-refactor) iterates in index order, which
		// is NOT the slice-sorted order when the original source had interlaced storage. The `sortedLeg` bitmap,
		// however, is authoritative regardless of iteration order — so filter by it rather than relying on a
		// positional prefix/suffix split.
		case StreamStrategy.SORTED_SUB -> column.stream()
				.filter(s -> sortedLeg.test(s.getSlice()))
				.map(s -> SliceAndMeasure.<T>builder()
						.slice(indexToSlice.apply(s.getSlice()))
						.valueProvider(s.getValueProvider())
						.build());
		case StreamStrategy.SORTED_SUB_COMPLEMENT -> column.stream()
				.filter(s -> !sortedLeg.test(s.getSlice()))
				.map(s -> SliceAndMeasure.<T>builder()
						.slice(indexToSlice.apply(s.getSlice()))
						.valueProvider(s.getValueProvider())
						.build());
		};
	}

	@Override
	public long size(StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL -> size();
		// The sorted leg is at the beginning of the column
		case StreamStrategy.SORTED_SUB -> sortedLength;
		// The complement of the sorted leg is after the sorted leg
		case StreamStrategy.SORTED_SUB_COMPLEMENT -> column.size() - sortedLength;
		};
	}

	@Override
	public IConsumingStream<T> keyStream() {
		return column.keyStream().map(indexToSlice::apply);
	}

	@Override
	public IValueProvider onValue(T key) {
		return column.onValue(sliceToIndex.applyAsInt(key));
	}

	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	@Override
	public IValueProvider onValue(T key, StreamStrategy strategy) {
		// Strict, symmetric semantics: indices [0, sortedPrefixLength) are the sorted leg; indices
		// [sortedPrefixLength, size) are the unordered complement. SORTED_SUB returns the value only for prefix
		// slices; SORTED_SUB_COMPLEMENT returns the value only for tail slices. ALL returns the value regardless.
		int index = sliceToIndex.applyAsInt(key);

		if (index < 0) {
			return IValueProvider.NULL;
		}

		return switch (strategy) {
		case StreamStrategy.ALL -> column.onValue(index);
		case StreamStrategy.SORTED_SUB -> sortedLeg.test(index) ? column.onValue(index) : IValueProvider.NULL;
		case StreamStrategy.SORTED_SUB_COMPLEMENT ->
			!sortedLeg.test(index) ? column.onValue(index) : IValueProvider.NULL;
		};
	}

	/**
	 * Forwards {@link ICompactable#compact()} to the wrapped inner column when it supports compaction. This lets the
	 * dictionarized aggregation pipeline (e.g. {@code AggregatingColumnsDistinct}) trim its storage to the tight
	 * post-population layout without the caller having to unwrap the {@link UndictionarizedColumn}. When the inner
	 * column is not itself {@link ICompactable} (or is the adapter around a non-compactable generic column), this is a
	 * no-op.
	 */
	@Override
	public void compact() {
		if (column instanceof ICompactable compactable) {
			compactable.compact();
		}
	}

	@Override
	public String toString() {
		return stream().toList()
				.stream()
				.limit(AdhocUnsafe.getLimitOrdinalToString())
				.map(sm -> sm.getSlice() + "=" + IValueProvider.getValue(sm.getValueProvider()))
				.collect(Collectors.joining(" "));
	}
}
