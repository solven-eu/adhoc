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
import eu.solven.adhoc.measure.transformers.Aggregator;
import lombok.Value;

/**
 * A data-structure associating each {@link Aggregator} with a {@link MultiTypeStorage}
 * 
 * @param <T>
 */
@Value
public class AggregatingMeasurators<T> {

	Map<Aggregator, MultiTypeStorage<T>> aggregatorToStorage = new HashMap<>();

	IOperatorsFactory transformationFactory;

	public void contribute(Aggregator aggregator, T key, Object v) {
		String aggregationKey = aggregator.getAggregationKey();
		IAggregation agg = transformationFactory.makeAggregation(aggregationKey);

		MultiTypeStorage<T> storage = aggregatorToStorage.computeIfAbsent(aggregator,
				k -> MultiTypeStorage.<T>builder().aggregation(agg).build());

		storage.merge(key, v);
	}

	public long size(Aggregator aggregator) {
		MultiTypeStorage<T> storage = aggregatorToStorage.get(aggregator);
		if (storage == null) {
			return 0L;
		} else {
			return storage.size();
		}
	}
}
