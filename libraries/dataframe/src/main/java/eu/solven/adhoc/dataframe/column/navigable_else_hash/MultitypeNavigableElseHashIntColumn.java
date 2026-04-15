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

import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeIntColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeIntColumnFastGetSorted;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashIntColumn;
import eu.solven.adhoc.dataframe.column.navigable.IHasSortedLeg;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableIntColumn;
import eu.solven.adhoc.dataframe.join.UnderlyingQueryStepHelpersNavigableElseHash;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.AccessLevel;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link IMultitypeColumnFastGet} enabling to fallback on hash-based {@link IMultitypeColumnFastGet}.
 * 
 * @author Benoit Lacelle
 * @see UnderlyingQueryStepHelpersNavigableElseHash
 */
@SuperBuilder
@Slf4j
@ToString
public class MultitypeNavigableElseHashIntColumn extends AMultitypeNavigableElseHashColumn<Integer>
		implements IMultitypeIntColumnFastGet, ICompactable, IHasSortedLeg {
	@Default
	@NonNull
	@Getter(AccessLevel.PROTECTED)
	final IMultitypeIntColumnFastGetSorted navigable = MultitypeNavigableIntColumn.builder().build();

	@Default
	@NonNull
	@Getter(AccessLevel.PROTECTED)
	final IMultitypeIntColumnFastGet hash = MultitypeHashIntColumn.builder().build();

	@Override
	public IMultitypeIntColumnFastGet purgeAggregationCarriers() {
		return MultitypeNavigableElseHashIntColumn.builder()
				.navigable(getNavigable().purgeAggregationCarriers())
				.hash(getHash().purgeAggregationCarriers())
				.build();
	}

	@Override
	public IValueReceiver append(int slice) {
		Optional<IValueReceiver> navigableReceiver = getNavigable().appendIfOptimal(slice);

		return navigableReceiver.orElseGet(() -> getHash().append(slice));
	}

	@SuppressWarnings("CPD-START")
	@Override
	public IValueProvider onValue(int key) {
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
	public IValueProvider onValue(int key, StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL -> onValue(key);
		// The navigable side IS the sorted leg.
		case StreamStrategy.SORTED_SUB -> getNavigable().onValue(key);
		// The hash side IS the unordered complement.
		case StreamStrategy.SORTED_SUB_COMPLEMENT -> getHash().onValue(key);
		};
	}
}
