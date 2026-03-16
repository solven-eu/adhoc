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
package eu.solven.adhoc.map;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.RowSliceFactory;
import lombok.experimental.UtilityClass;

/**
 * Helper methods related to {@link Map} in the context of Adhoc.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocMapHelpers {

	@Deprecated(since = "Provide your own ISliceFactory")
	public static IAdhocMap fromMap(Map<String, ?> asMap) {
		return fromMap(RowSliceFactory.builder().build(), asMap);
	}

	public static IAdhocMap fromMap(ISliceFactory factory, Map<String, ?> asMap) {
		if (asMap instanceof IAdhocMap adhocMap && adhocMap.getFactory().equals(factory)) {
			return adhocMap;
		}
		// BEWARE This assumes iterating along keys and along values follows the same order as entries.
		IMapBuilderPreKeys builder = factory.newMapBuilder(asMap.keySet());
		asMap.values().forEach(builder::append);
		return builder.build();
	}

	// BEWARE This is a very inefficient implementation
	public static int compareMap(IAdhocMap left, IAdhocMap right) {
		NavigableMap<String, Object> leftSorted = new TreeMap<>(left);
		NavigableMap<String, Object> rightSorted = new TreeMap<>(right);

		int compareKeySet = AdhocMapComparisonHelpers.compareKeySet(leftSorted.keySet().iterator(),
				rightSorted.keySet().iterator());

		if (compareKeySet != 0) {
			return compareKeySet;
		}

		return AdhocMapComparisonHelpers.compareValues2(leftSorted.values().iterator(),
				rightSorted.values().iterator());
	}

	public static <K, V> Map<K, V> aggregateMaps(Map<?, ?> lAsMap, Map<?, ?> rAsMap) {
		if (lAsMap == null || lAsMap.isEmpty()) {
			return (Map<K, V>) rAsMap;
		} else if (rAsMap == null || rAsMap.isEmpty()) {
			return (Map<K, V>) lAsMap;
		} else {
			// BEWARE In case on conflict, ImmutableMap.builder() will throw
			return (Map<K, V>) ImmutableMap.builder().putAll(lAsMap).putAll(rAsMap).build();
		}
	}

}
