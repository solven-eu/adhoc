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
package eu.solven.adhoc.measure.aggregation.collection;

import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.measure.aggregation.IAggregation;

/**
 * Aggregate inputs as {@link AtomicLongMap}, doing the union as aggregation.
 * 
 * @author Benoit Lacelle
 */
public class AtomicLongMapAggregation implements IAggregation {

	public static final String KEY = "ATOMIC_LONG_MAP";

	@Override
	public AtomicLongMap<?> aggregate(Object l, Object r) {
		AtomicLongMap<?> lAsMap = asMap(l);
		AtomicLongMap<?> rAsMap = asMap(r);

		AtomicLongMap<?> merged = aggregateMaps(lAsMap, rAsMap);

		if (merged != null && merged.isEmpty()) {
			// We prefer a column of null than a column full of empty Maps
			return null;
		}

		return merged;
	}

	protected AtomicLongMap<?> asMap(Object o) {
		return (AtomicLongMap<?>) o;
	}

	public static AtomicLongMap<?> aggregateMaps(AtomicLongMap<?> lAsMap, AtomicLongMap<?> rAsMap) {
		if (lAsMap == null) {
			return rAsMap;
		} else if (rAsMap == null) {
			return lAsMap;
		} else {
			AtomicLongMap<Object> merged = AtomicLongMap.create();

			lAsMap.asMap().forEach(merged::addAndGet);
			rAsMap.asMap().forEach(merged::addAndGet);

			return merged;
		}
	}
}
