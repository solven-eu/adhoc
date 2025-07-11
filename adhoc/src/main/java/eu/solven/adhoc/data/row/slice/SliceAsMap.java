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
package eu.solven.adhoc.data.row.slice;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.data.column.ConstantMaskMultitypeColumn;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.map.AdhocMapHelpers;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.MapComparators;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * A simple {@link IAdhocSlice} based on a {@link Map}.
 * 
 * @author Benoit Lacelle
 */
public final class SliceAsMap implements IAdhocSlice, Comparable<SliceAsMap> {
	// This is guaranteed not to contain a null-ref, neither as key nor as value
	// Value can only be simple values: neither a Collection, not a IValueMatcher
	// Implementations is generally a AdhocMap
	final Map<String, ?> asMap;

	protected SliceAsMap(Map<String, ?> asMap) {
		this.asMap = asMap;
	}

	public static SliceAsMap fromMap(Map<String, ?> asMap) {
		asMap = asMap.entrySet()
				.stream()
				.collect(ImmutableMap.toImmutableMap(Map.Entry::getKey,
						e -> AdhocPrimitiveHelpers.normalizeValue(e.getValue())));

		// We make an immutable copy. It is even more necessary as `Map.of` would throw an NPE on `.contains(null)`
		Map<String, ?> safeMap = AdhocMap.copyOf(asMap);

		// This is very fast: keep the check as it is
		if (safeMap.containsValue(null)) {
			// BEWARE Should this be a legit case, handling NULL specifically?
			throw new IllegalArgumentException("A slice can not hold value=null. Were: %s".formatted(asMap));
		}

		// This is a bit slow: it is an assertions
		assert safeMap.values().stream().noneMatch(o -> o instanceof Collection<?>)
				: "A simpleSlice can not hold value=Collection<?>. Were: %s".formatted(asMap);

		// This is a bit slow: it is an assertions
		assert safeMap.values().stream().noneMatch(o -> o instanceof IValueMatcher)
				: "A simpleSlice can not hold value=IValueMatcher. Were: %s".formatted(asMap);

		return new SliceAsMap(safeMap);
	}

	@Override
	public Set<String> getColumns() {
		return asMap.keySet();
	}

	@Override
	public Optional<Object> optSliced(String column) {
		return Optional.ofNullable(asMap.get(column));
	}

	@Override
	public Map<String, Object> getCoordinates() {
		return AdhocMapHelpers.immutableCopyOf(asMap);
	}

	@Override
	public SliceAsMap getAdhocSliceAsMap() {
		return this;
	}

	@Override
	public Map<String, ?> optSliced(Set<String> columns) {
		// Keep requested columns ordering
		Map<String, Object> filters = new LinkedHashMap<>();

		columns.forEach(column -> {
			optSliced(column).ifPresent(filter -> filters.put(column, filter));
		});

		return filters;
	}

	@Override
	public IAdhocFilter asFilter() {
		return AndFilter.and(asMap);
	}

	@Override
	public int hashCode() {
		// The simplest hashCode as this become a hotspot on large queries
		return asMap.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj == null) {
			return false;
		} else if (getClass() != obj.getClass()) {
			return false;
		} else if (this.hashCode() != obj.hashCode()) {
			// This is not checked by `Objects.hashCode` while AdhocMap keep it in Cache
			return false;
		}
		SliceAsMap other = (SliceAsMap) obj;

		// if (!Objects.equals(asMap.keySet(), other.asMap.keySet())) {
		// return false;
		// }
		// for (String key : asMap.keySet()) {
		// if (!Objects.equals(asMap.get(key), other.asMap.get(key))) {
		// return false;
		// }
		// }
		// return true;

		return Objects.equals(asMap, other.asMap);
	}

	@Override
	@SuppressWarnings("PMD.LooseCoupling")
	public int compareTo(SliceAsMap o) {
		if (this.asMap instanceof IAdhocMap adhocMap && o.asMap instanceof AdhocMap otherAdhocMap) {
			return adhocMap.compareTo(otherAdhocMap);
		} else {
			return MapComparators.mapComparator().compare(this.asMap, o.asMap);
		}
	}

	@Override
	public String toString() {
		return "slice:" + asMap;
	}

	/**
	 * Typically used by {@link ConstantMaskMultitypeColumn}.
	 * 
	 * @param mask
	 *            must not overlap existing columns.
	 */
	// @Override
	public SliceAsMap addColumns(Map<String, ?> mask) {
		if (mask.isEmpty()) {
			return this;
		} else {
			ImmutableMap.Builder<String, Object> builder =
					ImmutableMap.builderWithExpectedSize(asMap.size() + mask.size());
			return fromMap(builder.putAll(asMap).putAll(mask).build());
		}
	}
}
