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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.measure.aggregation.IAggregation;

/**
 * Aggregate inputs as {@link Map}, doing the union as aggregation.
 * 
 * @param <K>
 * @param <V>
 * @author Benoit Lacelle
 */
public class MapAggregator<K, V> implements IAggregation {

	public static final String KEY = "UNION_MAP";

	public boolean acceptAggregate(Object o) {
		return o instanceof Map || o == null;
	}

	@Override
	public Map<K, V> aggregate(Object l, Object r) {
		Map<?, ?> lAsMap = (Map<?, ?>) l;
		Map<?, ?> rAsMap = (Map<?, ?>) r;

		return aggregateMaps(lAsMap, rAsMap);
	}

	public static <K, V> Map<K, V> aggregateMaps(Map<?, ?> lAsMap, Map<?, ?> rAsMap) {
		if (lAsMap == null) {
			return (Map<K, V>) rAsMap;
		} else if (rAsMap == null) {
			return (Map<K, V>) lAsMap;
		} else {
			// BEWARE In case on conflict, ImmutableMap.builder() will through
			return (Map<K, V>) ImmutableMap.builder().putAll(lAsMap).putAll(rAsMap).build();
		}
	}
}
