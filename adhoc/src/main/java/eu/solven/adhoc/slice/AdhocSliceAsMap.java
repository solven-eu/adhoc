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
import java.util.Set;

import com.google.common.collect.ImmutableMap;

/**
 * A simple {@link IAdhocSlice} based on a {@link Map}
 */
public class AdhocSliceAsMap implements IAdhocSlice {
	final Map<String, ?> asMap;

	public AdhocSliceAsMap(Map<String, ?> asMap) {
		this.asMap = asMap;

		if (asMap.containsValue(null)) {
			throw new IllegalArgumentException("A slice can not hold value=null. Were: %s".formatted(asMap));
		}
	}

	public static IAdhocSlice fromMap(Map<String, ?> asMap) {
		return new AdhocSliceAsMap(asMap);
	}

	@Override
	public Set<String> getColumns() {
		return asMap.keySet();
	}

	@Override
	public Object getFilter(String column) {
		Object filter = asMap.get(column);

		if (filter == null) {
			if (asMap.containsKey(column)) {
				// BEWARE Should this be a legit case, handling NULL specifically?
				throw new IllegalStateException("%s is sliced with null".formatted(column));
			} else {
				throw new IllegalArgumentException("%s is not a sliced column".formatted(column));
			}
		}

		return filter;
	}

	@Override
	public Map<String, ?> getCoordinates() {
		return ImmutableMap.copyOf(asMap);
	}
}
