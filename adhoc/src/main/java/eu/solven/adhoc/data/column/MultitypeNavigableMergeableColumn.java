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

import java.util.Arrays;
import java.util.List;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link IMultitypeColumn} relies on {@link List} and {@link Arrays}.
 * 
 * The key has to be {@link Comparable}, so that `stream().sorted()` is a no-op, for performance reasons.
 *
 * @param <T>
 */
@SuperBuilder
@Slf4j
public class MultitypeNavigableMergeableColumn<T extends Comparable<T>> extends MultitypeNavigableColumn<T>
		implements IMultitypeMergeableColumn<T> {

	@NonNull
	@Getter
	IAggregation aggregation;

	// When writing the value, we need to ensure it is prepared by the aggregation function
	protected Object cleanValue(Object v) {
		return aggregation.aggregate(v, null);
	}

	@Override
	protected IValueReceiver merge(int index) {
		checkNotLocked(keys.get(index));

		return new IValueReceiver() {
			@Override
			public void onLong(long input) {
				onValue(index, new IValueReceiver() {

					@Override
					public void onLong(long existingAggregate) {
						if (aggregation instanceof ILongAggregation longAggregation) {
							long newAggregate = longAggregation.aggregateLongs(existingAggregate, input);
							values.set(index).onLong(newAggregate);
						} else {
							Object newAggregate = aggregation.aggregate(existingAggregate, input);
							values.set(index).onObject(newAggregate);
						}
					}

					@Override
					public void onObject(Object existingAggregate) {
						Object newAggregate = aggregation.aggregate(existingAggregate, input);
						values.set(index).onObject(newAggregate);
					}
				});
			}

			@Override
			public void onObject(Object input) {
				onValue(index, new IValueReceiver() {

					@Override
					public void onLong(long existingAggregate) {
						Object newAggregate = aggregation.aggregate(existingAggregate, input);
						values.set(index).onObject(newAggregate);
					}

					@Override
					public void onObject(Object existingAggregate) {
						Object newAggregate = aggregation.aggregate(existingAggregate, input);
						values.set(index).onObject(newAggregate);
					}
				});
			}
		};
	}

	@Override
	public IValueReceiver merge(T key) {
		int index = getIndex(key);

		if (index < 0) {
			return append(key);
		}
		return merge(index);
	}
}
