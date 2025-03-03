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
package eu.solven.adhoc.storage;

import java.util.HashMap;
import java.util.Map;

import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.IAggregationCarrier.IHasCarriers;
import lombok.Value;

/**
 * A data-structure associating each {@link Aggregator} with a {@link MultiTypeStorageMergeable}
 * 
 * @param <T>
 */
@Value
public class AggregatingMeasurators<T> {

	Map<Aggregator, IMultitypeColumnMergeable<T>> aggregatorToStorage = new HashMap<>();

	IOperatorsFactory transformationFactory;

	/**
	 * Useful when the DB has not done the aggregation
	 * 
	 * @param aggregator
	 * @param key
	 * @param v
	 */
	public void contributeRaw(Aggregator aggregator, T key, Object v) {
		String aggregationKey = aggregator.getAggregationKey();
		IAggregation agg = transformationFactory.makeAggregation(aggregationKey);

		IMultitypeColumnMergeable<T> storage = aggregatorToStorage.computeIfAbsent(aggregator,
				k -> MultiTypeStorageMergeable.<T>builder().aggregation(agg).build());

		storage.merge(key).onObject(v);
	}

	/**
	 * Useful when the DB has done the aggregation
	 * 
	 * Especially important for {@link IHasCarriers}
	 * 
	 * @param aggregator
	 * @param key
	 * @param v
	 */
	public void contributeAggregate(Aggregator aggregator, T key, Object v) {
		String aggregationKey = aggregator.getAggregationKey();
		IAggregation agg = transformationFactory.makeAggregation(aggregationKey);

		IMultitypeColumnMergeable<T> storage = aggregatorToStorage.computeIfAbsent(aggregator,
				k -> MultiTypeStorageMergeable.<T>builder().aggregation(agg).build());

		if (agg instanceof IHasCarriers hasCarriers) {
			v = hasCarriers.wrap(v);
		}

		storage.merge(key, v);
	}

	public long size(Aggregator aggregator) {
		IMultitypeColumnMergeable<T> storage = aggregatorToStorage.get(aggregator);
		if (storage == null) {
			return 0L;
		} else {
			return storage.size();
		}
	}
}
