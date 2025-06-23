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
package eu.solven.adhoc.data.tabular;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.query.table.IAliasedAggregator;

/**
 * A tabular grid where cells can be aggregated.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
public interface IMultitypeMergeableGrid<T> {

	/**
	 * Typically used to prepare the slice only once, before contributing multiple aggregates.
	 * 
	 * @author Benoit Lacelle
	 */
	@FunctionalInterface
	interface IOpenedSlice {
		IValueReceiver contribute(IAliasedAggregator aggregator);
	}

	/**
	 * 
	 * @param key
	 * @return an {@link IOpenedSlice} into which multiple aggregators can be written
	 */
	IOpenedSlice openSlice(T key);

	/**
	 * 
	 * @param aggregator
	 * @param key
	 * @return a {@link IValueReceiver} into which an aggregate ha to be written for given aggregator and given key
	 */
	default IValueReceiver contribute(T key, IAliasedAggregator aggregator) {
		return openSlice(key).contribute(aggregator);
	}

	/**
	 * Will typically handle {@link IAggregationCarrier}.
	 * 
	 * @param aggregator
	 * @return the close {@link IMultitypeColumnFastGet}
	 */
	IMultitypeColumnFastGet<T> closeColumn(IAliasedAggregator aggregator);

	long size(IAliasedAggregator aggregator);

}
