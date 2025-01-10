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
package eu.solven.adhoc.slice;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

/**
 * A simple {@link IAdhocSlice} based on a {@link Map}
 */
public class AdhocSliceAsMap implements IAdhocSlice {
	final Map<String, ?> asMap;

	protected AdhocSliceAsMap(Map<String, ?> asMap) {
		this.asMap = asMap;
	}

	public static IAdhocSlice fromMap(Map<String, ?> asMap) {
		// We make an immutable copy. It is even more necessary as `Map.of` would throw an NPE on `.contains(null)`
		Map<String, ?> safeMap = ImmutableMap.copyOf(asMap);

		if (safeMap.containsValue(null)) {
			throw new IllegalArgumentException("A slice can not hold value=null. Were: %s".formatted(asMap));
		}

		return new AdhocSliceAsMap(safeMap);
	}

	@Override
	public Set<String> getColumns() {
		return asMap.keySet();
	}

	@Override
	public Optional<Object> optFilter(String column) {
		Object filter = asMap.get(column);

		if (filter == null) {
			if (asMap.containsKey(column)) {
				// BEWARE Should this be a legit case, handling NULL specifically?
				throw new IllegalStateException("%s is sliced with NULL".formatted(column));
			} else {
				// throw new IllegalStateException("%s is sliced with null".formatted(column));
				return Optional.empty();
			}
		}

		return Optional.of(filter);
	}

	@Override
	public Map<String, ?> getCoordinates() {
		return ImmutableMap.copyOf(asMap);
	}
}
