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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import lombok.EqualsAndHashCode;

/**
 * A simple {@link IAdhocSlice} based on a {@link Map}
 */
@EqualsAndHashCode
public class AdhocSliceAsMap implements IAdhocSlice {
	final Map<String, ?> asMap;

	protected AdhocSliceAsMap(Map<String, ?> asMap) {
		this.asMap = asMap;
	}

	public static AdhocSliceAsMap fromMap(Map<String, ?> asMap) {
		// We make an immutable copy. It is even more necessary as `Map.of` would throw an NPE on `.contains(null)`
		Map<String, ?> safeMap = ImmutableMap.copyOf(asMap);

		if (safeMap.containsValue(null)) {
			// BEWARE Should this be a legit case, handling NULL specifically?
			throw new IllegalArgumentException("A slice can not hold value=null. Were: %s".formatted(asMap));
		} else if (safeMap.values().stream().anyMatch(o -> o instanceof Collection<?>)) {
			throw new IllegalArgumentException(
					"A simpleSlice can not hold value=Collection<?>. Were: %s".formatted(asMap));
		}

		return new AdhocSliceAsMap(safeMap);
	}

	@Override
	public Set<String> getColumns() {
		return asMap.keySet();
	}

	@Override
	public Optional<Object> optFilter(String column) {
		return Optional.ofNullable(asMap.get(column));
	}

	@Override
	public Map<String, Object> getCoordinates() {
		return ImmutableMap.copyOf(asMap);
	}

	@Override
	public AdhocSliceAsMap getAdhocSliceAsMap() {
		return this;
	}

	@Override
	public Map<String, ?> optFilters(Set<String> columns) {
		// Keep requested columns ordering
		Map<String, Object> filters = new LinkedHashMap<>();

		columns.forEach(column -> {
			optFilter(column).ifPresent(filter -> filters.put(column, filter));
		});

		return filters;
	}
}
