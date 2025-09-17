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

import java.util.Map;
import java.util.stream.Stream;

import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * This is similar to a {@link Map}, but it is specialized for full-scan read operations and `.append`. If you need
 * `.merge()` capabilities, consider {@link IMultitypeMergeableColumn}.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
public interface IMultitypeColumn<T> {

	/**
	 * 
	 * @return the number of different keys.
	 */
	long size();

	/**
	 * 
	 * @return true if this column has size==0.
	 */
	boolean isEmpty();

	/**
	 * Typically called by {@link CubeQueryEngine} once an {@link ITableWrapper} measure is fully received, to turn
	 * {@link IAggregationCarrier} into the real aggregate (e.g. turning a CountCarrier, holding a long, to be
	 * differentiated with a column holding longs).
	 */
	// TODO This design is broken: If the AVG is result is not returned to the compositeCube, but is underlying
	// to another measure, it should be unWrapped (even in presence of AGGREGATION_CARRIERS_STAY_WRAPPED).
	// BEWARE: What if given column if both used as underlying of a local measure, and returned to the composite
	// cube? it should be both closed and not closed.
	IMultitypeColumn<T> purgeAggregationCarriers();

	/**
	 * Append-else-merge.
	 * 
	 * @param slice
	 * @return the {@link IValueReceiver} into which the value has to be written. If the key already exists, switch to
	 *         an optional merge. Appending null is generally a no-op, not a remove.
	 */
	IValueReceiver append(T slice);

	@Deprecated(since = "Should rely on `IValueConsumer append(T slice)`")
	default void append(T slice, Object o) {
		append(slice).onObject(o);
	}

	void scan(IColumnScanner<T> rowScanner);

	@Deprecated(since = "It seems useless", forRemoval = true)
	<U> Stream<U> stream(IColumnValueConverter<T, U> converter);

	Stream<SliceAndMeasure<T>> stream();

	/**
	 * 
	 * @param strategy
	 * @return a {@link Stream} with the requested strategy
	 */
	default Stream<SliceAndMeasure<T>> stream(StreamStrategy strategy) {
		return defaultStream(this, strategy);
	}

	/**
	 * 
	 * @param <T>
	 * @param column
	 * @param stragegy
	 * @return a valid (yet possibly not optimal) {@link Stream} given the strategy, making no assumption on the column.
	 */
	@SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
	static <T> Stream<SliceAndMeasure<T>> defaultStream(IMultitypeColumn<T> column, StreamStrategy stragegy) {
		return switch (stragegy) {
		case StreamStrategy.ALL:
			yield column.stream();
		case StreamStrategy.SORTED_SUB:
			// Assume there is no sorted leg
			yield Stream.empty();
		case StreamStrategy.SORTED_SUB_COMPLEMENT:
			// As we assume there is no sorted leg, the complement is all
			yield column.stream();
		default:
			throw new IllegalArgumentException("Unexpected value: " + stragegy);
		};
	}

	Stream<T> keyStream();

}
