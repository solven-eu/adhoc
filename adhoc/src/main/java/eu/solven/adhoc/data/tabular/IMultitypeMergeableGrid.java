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
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.IAggregationCarrier;
import eu.solven.adhoc.measure.sum.IAggregationCarrier.IHasCarriers;

/**
 * A tabular grid where cells can be aggregated.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
public interface IMultitypeMergeableGrid<T> {

	/**
	 * Useful when the DB has done the aggregation
	 * 
	 * Especially important for {@link IHasCarriers}
	 * 
	 * @param aggregator
	 * @param key
	 * @param v
	 */
	@Deprecated(since = "Prefer `IValueConsumer contributePre(Aggregator aggregator, T key)`")
	default void contributePre(Aggregator aggregator, T key, Object v) {
		contributePre(aggregator, key).onObject(v);
	}

	IValueReceiver contributePre(Aggregator aggregator, T key);

	/**
	 * Will typically handle {@link IAggregationCarrier}.
	 * 
	 * @param aggregator
	 * @return the close {@link IMultitypeColumnFastGet}
	 */
	IMultitypeColumnFastGet<T> closeColumn(Aggregator aggregator);

}
