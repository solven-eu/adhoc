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

import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.cuboid.slice.Slice;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.stream.IConsumingStream;

/**
 * For {@link IMultitypeColumn} which enables fast `.get` operations.
 * 
 * @param <T>
 *            typically {@link Slice}
 * @author Benoit Lacelle
 */
public interface IMultitypeColumnFastGet<T> extends IMultitypeColumn<T> {

	IConsumingStream<SliceAndMeasure<T>> stream();

	IConsumingStream<SliceAndMeasure<T>> limit(int limit);

	IConsumingStream<SliceAndMeasure<T>> skip(int skip);

	/**
	 *
	 * @param strategy
	 * @return an {@link IConsumingStream} with the requested strategy
	 */
	default IConsumingStream<SliceAndMeasure<T>> stream(StreamStrategy strategy) {
		return defaultStream(this, strategy);
	}

	/**
	 *
	 * @param <T>
	 * @param column
	 * @param strategy
	 * @return a valid (yet possibly not optimal) {@link IConsumingStream} given the strategy, making no assumption on
	 *         the column.
	 */
	static <T> IConsumingStream<SliceAndMeasure<T>> defaultStream(IMultitypeColumnFastGet<T> column,
			StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL:
			// As we assume there is no sorted leg, the complement is all
		case StreamStrategy.SORTED_SUB_COMPLEMENT:
			yield column.stream();
		case StreamStrategy.SORTED_SUB:
			// Assume there is no sorted leg
			yield IConsumingStream.empty();
		};
	}

	/**
	 * Similar to a `.get` but the value is available through a {@link IValueReceiver}
	 * 
	 * @param slice
	 * @param valueReceiver
	 */
	@Deprecated(since = "Prefer `IValueProvider onValue(T key)`")
	default void onValue(T slice, IValueReceiver valueReceiver) {
		onValue(slice).acceptReceiver(valueReceiver);
	}

	/**
	 * 
	 * @param key
	 * @return an {@link IValueReceiver} to accept a value for given key. If the value is null, the operation should be
	 *         without effect.
	 */
	IValueProvider onValue(T key);

	default IValueProvider onValue(T key, StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL:
			// As we assume there is no sorted leg, the complement is all
		case StreamStrategy.SORTED_SUB_COMPLEMENT:
			yield onValue(key);
		case StreamStrategy.SORTED_SUB:
			// Assume there is no sorted leg
			yield IValueProvider.NULL;
		};
	}

	default long size(StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL:
			// As we assume there is no sorted leg, the complement is all
		case StreamStrategy.SORTED_SUB_COMPLEMENT:
			yield size();
		case StreamStrategy.SORTED_SUB:
			// Assume there is no sorted leg
			yield 0;
		};
	}

	@Override
	IMultitypeColumnFastGet<T> purgeAggregationCarriers();
}
