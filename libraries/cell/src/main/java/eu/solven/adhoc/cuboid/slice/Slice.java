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
package eu.solven.adhoc.cuboid.slice;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.SimpleAndFilter;
import eu.solven.adhoc.filter.value.NullMatcher;
import eu.solven.adhoc.map.AdhocMapHelpers;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.IHasAdhocMap;
import eu.solven.adhoc.map.MapComparators;
import eu.solven.adhoc.map.MaskedAdhocMap;
import eu.solven.adhoc.map.factory.ISliceFactory;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A simple {@link ISlice} based on a {@link Map}. It may be kept as use for small queries, or later compressed (e.g.
 * into columnar storage) in case of many slices.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public final class Slice implements ISlice {
	@Getter
	final ISliceFactory factory;

	// This is guaranteed not to contain a null-ref, neither as key nor as value
	// Value can only be simple values: neither a Collection, not a IValueMatcher
	// Implementations is generally a AdhocMap
	final IAdhocMap asMap;

	/**
	 * Assume the values are already normalized
	 * 
	 * @param adhocMap
	 * @return
	 */
	public static ISlice fromMapUnsafe(IAdhocMap adhocMap) {
		return new Slice(adhocMap.getFactory(), adhocMap);
	}

	@Override
	public IAdhocMap asAdhocMap() {
		return asMap;
	}

	@Override
	public boolean isEmpty() {
		return asMap.isEmpty();
	}

	@Override
	public Object getGroupBy(String column) {
		if (asAdhocMap().containsKey(column)) {
			return explicitNull(asAdhocMap().get(column));
		} else {
			throw new IllegalArgumentException(
					"%s is not a sliced column, amongst %s".formatted(column, columnsKeySet()));
		}
	}

	@Override
	public Optional<Object> optGroupBy(String column) {
		return Optional.ofNullable(explicitNull(asMap.get(column)));
	}

	@Override
	public Map<String, ?> getCoordinates() {
		return Maps.transformValues(asMap, this::explicitNull);
	}

	Object explicitNull(Object v) {
		return NullMatcher.unwrapNull(v);
	}

	@Override
	public ISliceFilter asFilter() {
		return SimpleAndFilter.of(asMap);
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
		} else if (this.hashCode() != obj.hashCode()) {
			// This is not checked by `Objects.hashCode` while AdhocMap keep it in Cache
			return false;
		}

		if (obj instanceof IHasAdhocMap otherSlice) {
			return Objects.equals(this.asAdhocMap(), otherSlice.asAdhocMap());
		} else if (obj instanceof ISlice otherSlice) {
			return Objects.equals(this.getCoordinates(), otherSlice.asAdhocMap());
		} else {
			return false;
		}
	}

	@Override
	public int compareTo(ISlice o) {
		if (o == null) {
			throw new IllegalArgumentException("null");
		} else if (o instanceof IHasAdhocMap otherSlice) {
			return this.asAdhocMap().compareTo(otherSlice.asAdhocMap());
		} else {
			return MapComparators.mapComparator().compare(this.asAdhocMap(), o.asAdhocMap());
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
	@Override
	public ISlice addColumns(Map<String, ?> mask) {
		if (mask.isEmpty()) {
			return this;
		} else if (!Sets.intersection(mask.keySet(), asAdhocMap().keySet()).isEmpty()) {
			throw new IllegalArgumentException("Conflicting key between slice=%s and mask=%s".formatted(this, mask));
		} else {
			// This branch has to be optimized as we tend to generate large number of slice with such masked columns
			return MaskedAdhocMap.builder().decorated(asMap).mask(mask).build().asSlice();
		}
	}

	@Override
	public ISlice retainAll(NavigableSet<String> columns) {
		return AdhocMapHelpers.fromMap(factory, asMap.retainAll(columns)).asSlice();
	}

}
