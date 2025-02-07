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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import autovalue.shaded.com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;

// `extends AbstractMap` enables not duplicating `.toString`
public final class AdhocMap extends AbstractMap<String, Object> implements IAdhocMap {
	// This is mandatory for fast `.get`
	final AdhocObject2IntArrayMap<String> keyToIndex;
	// final List<String> keys;
	final List<Object> values;

	// private Set<String> keysAsSet;

	/** Cache the hash code for the string */
	// Like String
	private int hash; // Default to 0

	/**
	 * Cache if the hash has been calculated as actually being zero, enabling us to avoid recalculating this.
	 */
	// Like String
	private boolean hashIsZero; // Default to false;

	private AdhocMap(AdhocObject2IntArrayMap<String> keyToIndex, List<Object> values) {
		this.keyToIndex = keyToIndex;
		this.values = values;
	}

	@Override
	public int size() {
		return values.size();
	}

	@Override
	public boolean isEmpty() {
		return values.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return keyToIndex.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		// This is a slow operation
		return values.contains(value);
	}

	@Override
	public Object get(Object key) {
		if (key instanceof String keyAsString) {
			int index = keyToIndex.getInt(keyAsString);
			if (index >= 0) {
				return values.get(index);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	@Override
	public Object put(String key, Object value) {
		throw new UnsupportedOperationException("This is immutable");
	}

	@Override
	public Object remove(Object key) {
		throw new UnsupportedOperationException("This is immutable");
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		throw new UnsupportedOperationException("This is immutable");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("This is immutable");
	}

	@Override
	public Set<String> keySet() {
		// if (keysAsSet == null) {
		// keysAsSet = ImmutableSet.copyOf(keys);
		// assert keysAsSet.size() == keys.size() : ".keySet is not distinct";
		// }

		return Collections.unmodifiableSet(keyToIndex.keySet());
	}

	@Override
	public Collection<Object> values() {
		return Collections.unmodifiableCollection(values);
	}

	@Override
	public Set<Map.Entry<String, Object>> entrySet() {
		// Does this needs to be cached?
		return Streams.mapWithIndex(keySet().stream(), (key, index) -> {
			Object value = values.get(Ints.checkedCast(index));
			return Map.entry(key, value);
		}).collect(Collectors.toSet());
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>
	 * Computes hash code consistent with {@link java.util.Map.Entry#hashCode()}.
	 */
	@Override
	public int hashCode() {
		// Like String.hashCode
		int h = hash;
		if (h == 0 && !hashIsZero) {
			int[] hashcodeHolder = new int[1];

			Object2IntMaps.fastForEach(keyToIndex, entry -> {
				String key = entry.getKey();
				int index = entry.getIntValue();
				// see `Map.Entry#hashCode`
				hashcodeHolder[0] += key.hashCode() ^ values.get(index).hashCode();
			});

			h = hashcodeHolder[0];

			if (h == 0) {
				hashIsZero = true;
			} else {
				hash = h;
			}
		}
		return h;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (obj instanceof AdhocMap objAsMap) {
			// hashCode is cached: while many .equals are covered by a previous hashCode check, it may be only partial
			// (e.g. in a hashMap, we do .equals between instance for each hashCodes are equals modulo X)
			if (this.hashCode() != objAsMap.hashCode()) {
				return false;
			} else

			// If not equals, it is most probably spot by value than by keys
			// In other words: there is high probability of same keys and different values than different keys but
			// same values. `Objects.equals` will do a reference check
			if (!Objects.equals(values, objAsMap.values)) {
				return false;
			} else if (keyToIndex == objAsMap.keyToIndex) {
				// Same key ref
				return true;
			} else
			// No need to check for indexed
			if (!Objects.equals(keyToIndex.keySet(), objAsMap.keyToIndex.keySet())) {
				return false;
			} else {
				return true;
			}
		} else if (obj instanceof Map<?, ?>) {
			// Rely on the other class .equals: BEWARE, this can lead to StackOverFlow
			return obj.equals(this);
		} else {
			return false;
		}
	}

	// @Override
	// public int compareTo(AdhocMap o) {
	// // TODO Auto-generated method stub
	// return 0;
	// }

	public static class AdhocMapBuilder {
		final NavigableSet<String> keys;
		final List<Object> values;

		private AdhocMapBuilder(Collection<String> keys) {
			this.keys = toNavigableSet(keys);
			this.values = new ArrayList<>(keys.size());
		}

		private static NavigableSet<String> toNavigableSet(Collection<String> keys) {
			if (keys instanceof NavigableSet<String> navigableKeys) {
				return navigableKeys;
			}
			return new TreeSet<>(keys);
		}

		public AdhocMapBuilder append(Object value) {
			values.add(value);

			return this;
		}

		public IAdhocMap build() {
			AdhocObject2IntArrayMap<String> keyToIndex = new AdhocObject2IntArrayMap<>(keys.size());

			// -1 is not a valid index: it is a good default value (better than the defaultDefault 0)
			keyToIndex.defaultReturnValue(-1);

			keys.forEach(key -> keyToIndex.put(key, keyToIndex.size()));

			return new AdhocMap(keyToIndex, values);
		}
	}

	/**
	 * 
	 * @param keys
	 *            the ordered-set of keys which will be appended
	 * @return
	 */
	public static AdhocMapBuilder builder(Set<String> keys) {
		return new AdhocMapBuilder(keys);
	}

	public static Map<String, Object> immutableCopyOf(Map<String, ?> map) {
		if (map instanceof IAdhocMap adhocMap) {
			// For performance, we expect to be generally in this branch
			return adhocMap;
		} else {
			return ImmutableMap.copyOf(map);
		}
	}
}
