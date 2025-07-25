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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import eu.solven.adhoc.data.column.ConstantMaskMultitypeColumn;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.ISliceFactory;
import eu.solven.adhoc.map.MapComparators;
import eu.solven.adhoc.map.StandardSliceFactory;
import eu.solven.adhoc.map.StandardSliceFactory.MapBuilderThroughKeys;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A simple {@link IAdhocSlice} based on a {@link Map}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public final class SliceAsMap implements IAdhocSlice {
	@Getter
	final ISliceFactory factory;

	// This is guaranteed not to contain a null-ref, neither as key nor as value
	// Value can only be simple values: neither a Collection, not a IValueMatcher
	// Implementations is generally a AdhocMap
	final Map<String, ?> asMap;

	@Deprecated(since = "Should use a ISliceFactory")
	public static IAdhocSlice fromMap(Map<String, ?> asMap) {
		return fromMap(StandardSliceFactory.builder().build(), asMap);
	}

	public static IAdhocSlice fromMap(ISliceFactory factory, Map<String, ?> asMap) {
		MapBuilderThroughKeys builder = factory.newMapBuilder();

		asMap.forEach(builder::put);
		return builder.build().asSlice();
	}

	/**
	 * Assume the values are already normalized
	 * 
	 * @param adhocMap
	 * @return
	 */
	public static SliceAsMap fromMapUnsafe(IAdhocMap adhocMap) {
		return new SliceAsMap(adhocMap.getFactory(), adhocMap);
	}

	@Override
	public boolean isEmpty() {
		return asMap.isEmpty();
	}

	@Override
	public Set<String> getColumns() {
		return asMap.keySet();
	}

	@Override
	public Object getRawSliced(String column) {
		if (asMap.containsKey(column)) {
			return explicitNull(asMap.get(column));
		} else {
			throw new IllegalArgumentException("%s is not a sliced column, amongst %s".formatted(column, getColumns()));
		}
	}

	@Override
	public Optional<Object> optSliced(String column) {
		return Optional.ofNullable(asMap.get(column));
	}

	@Override
	public Map<String, ?> getCoordinates() {
		return Maps.transformValues(asMap, this::explicitNull);
	}

	private Object explicitNull(Object v) {
		if (v == NullMatcher.NULL_HOLDER) {
			return null;
		} else {
			return v;
		}
	}

	@Override
	public Map<String, ?> optSliced(Set<String> columns) {
		// Keep requested columns ordering
		Map<String, Object> filters = new LinkedHashMap<>();

		columns.forEach(column -> {
			optSliced(column).ifPresent(v -> filters.put(column, explicitNull(v)));
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

		return Objects.equals(asMap, other.asMap);
	}

	@Override
	public int compareTo(IAdhocSlice o) {
		if (o instanceof SliceAsMap otherSlice && this.asMap instanceof IAdhocMap thisAdhocMap
				&& otherSlice.asMap instanceof IAdhocMap otherAdhocMap) {
			return thisAdhocMap.compareTo(otherAdhocMap);
		}
		return MapComparators.mapComparator().compare(this.getCoordinates(), o.getCoordinates());
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
	@Override
	public IAdhocSlice addColumns(Map<String, ?> mask) {
		if (mask.isEmpty()) {
			return this;
		} else {
			// TODO There should probably be a branch dedicated for IAdhocMap, as we would generate generate many Map
			// with given mask.
			// if (asMap instanceof IAdhocMap)
			ImmutableMap.Builder<String, Object> builder =
					ImmutableMap.builderWithExpectedSize(asMap.size() + mask.size());
			return fromMap(builder.putAll(asMap).putAll(mask).build());
		}
	}

	public static SliceAsMap grandTotal() {
		return new SliceAsMap(StandardSliceFactory.builder().build(), ImmutableMap.of());
	}
}
