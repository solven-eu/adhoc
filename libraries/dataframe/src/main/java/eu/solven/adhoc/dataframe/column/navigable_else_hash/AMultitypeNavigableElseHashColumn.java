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

import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.IColumnValueConverter;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGetSorted;
import eu.solven.adhoc.dataframe.column.navigable.IHasSortedLeg;
import eu.solven.adhoc.dataframe.join.UnderlyingQueryStepHelpersNavigableElseHash;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.stream.IConsumingStream;
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
public abstract class AMultitypeNavigableElseHashColumn<T extends Comparable<T>>
		implements IMultitypeColumnFastGet<T>, ICompactable, IHasSortedLeg {

	// A first leg where slices are sorted
	protected abstract IMultitypeColumnFastGetSorted<T> getNavigable();

	// A second leg where slices are in random order
	// It must not contain any slice present is the navigable leg
	protected abstract IMultitypeColumnFastGet<T> getHash();

	/**
	 *
	 * @return the sum of the underlying sizes as we each slice can only be present in one of the underlying.
	 */
	@Override
	public long size() {
		return getNavigable().size() + getHash().size();
	}

	@Override
	public long size(StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL -> size();
		// The navigable side IS the sorted leg.
		case StreamStrategy.SORTED_SUB -> getNavigable().size();
		// The hash side IS the unordered complement.
		case StreamStrategy.SORTED_SUB_COMPLEMENT -> getHash().size();
		};
	}

	@Override
	public boolean isEmpty() {
		return getNavigable().isEmpty() && getHash().isEmpty();
	}

	@Override
	public long sortedPrefixLength() {
		return getNavigable().size();
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		getNavigable().scan(rowScanner);
		getHash().scan(rowScanner);
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		return Stream.concat(getNavigable().stream(converter), getHash().stream(converter));
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> stream() {
		return IConsumingStream.concat(ImmutableList.of(getNavigable().stream(), getHash().stream()));
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> limit(int limit) {
		if (limit > getNavigable().size()) {
			throw new IllegalArgumentException("limit=%s navigable.size=%s".formatted(limit, getNavigable().size()));
		}
		return getNavigable().limit(limit);
	}

	@Override
	public IConsumingStream<SliceAndMeasure<T>> skip(int skip) {
		if (skip > getNavigable().size()) {
			throw new IllegalArgumentException("skip=%s navigable.size=%s".formatted(skip, getNavigable().size()));
		}
		return IConsumingStream.concat(ImmutableList.of(getNavigable().skip(skip), getHash().stream()));
	}

	@Override
	public IConsumingStream<T> keyStream() {
		return IConsumingStream.concat(ImmutableList.of(getNavigable().keyStream(), getHash().keyStream()));
	}

	@Override
	public IValueReceiver append(T slice) {
		Optional<IValueReceiver> navigableReceiver = getNavigable().appendIfOptimal(slice, false);

		return navigableReceiver.orElseGet(() -> getHash().append(slice));
	}

	@Override
	public IValueProvider onValue(T key) {
		return valueReceiver -> {
			// We assume navigable has a higher probably to hold the value
			// TODO Have some strategy (e.g. based on size)
			getNavigable().onValue(key).acceptReceiver(new IValueReceiver() {

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
						getHash().onValue(key).acceptReceiver(valueReceiver);
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
		case StreamStrategy.SORTED_SUB -> getNavigable().onValue(key);
		// The hash side IS the unordered complement.
		case StreamStrategy.SORTED_SUB_COMPLEMENT -> getHash().onValue(key);
		};
	}

	@SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
	@Override
	public IConsumingStream<SliceAndMeasure<T>> stream(StreamStrategy stragegy) {
		return switch (stragegy) {
		case StreamStrategy.ALL -> this.stream();
		case StreamStrategy.SORTED_SUB -> getNavigable().stream();
		case StreamStrategy.SORTED_SUB_COMPLEMENT -> getHash().stream();
		default -> IMultitypeColumnFastGet.defaultStream(this, stragegy);
		};
	}

	@Override
	public void compact() {
		if (getNavigable() instanceof ICompactable compactable) {
			compactable.compact();
		}
		if (getHash() instanceof ICompactable compactable) {
			compactable.compact();
		}
	}
}
