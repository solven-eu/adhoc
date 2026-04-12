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
package eu.solven.adhoc.dataframe.column.navigable_else_hash;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.IColumnValueConverter;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGetSorted;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.dataframe.column.navigable.IHasSortedLeg;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableColumn;
import eu.solven.adhoc.dataframe.join.UnderlyingQueryStepHelpersNavigableElseHash;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.stream.IConsumingStream;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link IMultitypeColumnFastGet} enabling to fallback on hash-based {@link IMultitypeColumnFastGet}.
 * 
 * @param <T>
 * @author Benoit Lacelle
 * @see UnderlyingQueryStepHelpersNavigableElseHash
 */
@SuperBuilder
@Slf4j
@ToString
public class MultitypeNavigableElseHashColumn<T extends Comparable<T>>
		implements IMultitypeColumnFastGet<T>, ICompactable, IHasSortedLeg {
	@Default
	@NonNull
	final IMultitypeColumnFastGetSorted<T> navigable = MultitypeNavigableColumn.<T>builder().build();

	@Default
	@NonNull
	final IMultitypeColumnFastGet<T> hash = MultitypeHashColumn.<T>builder().build();

	/**
	 *
	 * @return the sum of the underlying sizes as we each slice can only be present in one of the underlying.
	 */
	@Override
	public long size() {
		return navigable.size() + hash.size();
	}

	@Override
	public boolean isEmpty() {
		return navigable.isEmpty() && hash.isEmpty();
	}

	@Override
	public long sortedPrefixLength() {
		return navigable.size();
	}

	@Override
	public IMultitypeColumnFastGet<T> purgeAggregationCarriers() {
		navigable.purgeAggregationCarriers();
		hash.purgeAggregationCarriers();

		return MultitypeNavigableElseHashColumn.<T>builder()
				.navigable(navigable.purgeAggregationCarriers())
				.hash(hash.purgeAggregationCarriers())
				.build();
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		navigable.scan(rowScanner);
		hash.scan(rowScanner);
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		return Stream.concat(navigable.stream(converter), hash.stream(converter));
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> stream() {
		return IConsumingStream.concat(List.of(navigable.stream(), hash.stream()));
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> limit(int limit) {
		if (limit > navigable.size()) {
			throw new IllegalArgumentException("limit=%s navigable.size=%s".formatted(limit, navigable.size()));
		}
		return navigable.limit(limit);
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> skip(int skip) {
		if (skip > navigable.size()) {
			throw new IllegalArgumentException("skip=%s navigable.size=%s".formatted(skip, navigable.size()));
		}
		return IConsumingStream.concat(List.of(navigable.skip(skip), hash.stream()));
	}

	@Override
	public IConsumingStream<T> keyStream() {
		return IConsumingStream.concat(List.of(navigable.keyStream(), hash.keyStream()));
	}

	@Override
	public IValueReceiver append(T slice) {
		Optional<IValueReceiver> navigableReceiver = navigable.appendIfOptimal(slice);

		return navigableReceiver.orElseGet(() -> hash.append(slice));
	}

	@Override
	public IValueProvider onValue(T key) {
		return valueReceiver -> {
			// We assume navigable has a higher probably to hold the value
			// TODO Have some strategy (e.g. based on size)
			navigable.onValue(key).acceptReceiver(new IValueReceiver() {

				@Override
				public void onLong(long v) {
					valueReceiver.onLong(v);
				}

				@Override
				public void onDouble(double v) {
					valueReceiver.onDouble(v);
				}

				@Override
				public void onObject(Object v) {
					if (v == null) {
						// No value from navigable: let's try from hash
						hash.onValue(key).acceptReceiver(valueReceiver);
					} else {
						valueReceiver.onObject(v);
					}
				}
			});
		};
	}

	@Override
	public IValueProvider onValue(T key, StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL -> onValue(key);
		// The navigable side IS the sorted leg.
		case StreamStrategy.SORTED_SUB -> navigable.onValue(key);
		// The hash side IS the unordered complement.
		case StreamStrategy.SORTED_SUB_COMPLEMENT -> hash.onValue(key);
		};
	}

	@SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
	@Override
	public IConsumingStream<SliceAndMeasure<T>> stream(StreamStrategy stragegy) {
		return switch (stragegy) {
		case StreamStrategy.ALL -> this.stream();
		case StreamStrategy.SORTED_SUB -> navigable.stream();
		case StreamStrategy.SORTED_SUB_COMPLEMENT -> hash.stream();
		default -> IMultitypeColumnFastGet.defaultStream(this, stragegy);
		};
	}

	@Override
	public void compact() {
		if (navigable instanceof ICompactable compactable) {
			compactable.compact();
		}
		if (hash instanceof ICompactable compactable) {
			compactable.compact();
		}
	}
}
