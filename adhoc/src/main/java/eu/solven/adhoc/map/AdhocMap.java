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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Streams;
import com.google.common.primitives.Ints;

import it.unimi.dsi.fastutil.objects.AbstractObject2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

// `extends AbstractMap` enables not duplicating `.toString`
public final class AdhocMap extends AbstractMap<String, Object> implements IAdhocMap {
	// This is mandatory for fast `.get`
	// This is sorted
	final Object2IntArrayMap<String> keyToIndex;
	// final List<String> keys;
	final List<Object> values;

	/** Cache the hash code for the string */
	// Like String
	private int hash; // Default to 0

	/**
	 * Cache if the hash has been calculated as actually being zero, enabling us to avoid recalculating this.
	 */
	// Like String
	private boolean hashIsZero; // Default to false;

	private static Comparator<Object> valueComparator = new ComparableElseClassComparatorV2();

	private AdhocMap(Object2IntArrayMap<String> keyToIndex, List<Object> values) {
		assert Ordering.natural().isOrdered(keyToIndex.keySet());

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
		// hashCode caching Like String.hashCode
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

	// Looks a lot like NavigableMapComparator. Duplication?
	@Override
	public int compareTo(AdhocMap o) {
		// throw new NotYetImplementedException("%s.compareTo".formatted(getClass()));
		if (this == o) {
			// Typically happens when iterating along queryStep underlyings, as we often expect 2 underlyings to provide
			// same sliceAsMap
			return 0;
		}

		ObjectIterator<Object2IntMap.Entry<String>> thisIterator = keyToIndex.object2IntEntrySet().fastIterator();
		ObjectIterator<Object2IntMap.Entry<String>> otherIterator = o.keyToIndex.object2IntEntrySet().fastIterator();

		// Loop until the iterator has values.
		while (true) {
			boolean thisHashNext = thisIterator.hasNext();
			boolean otherhashnext = otherIterator.hasNext();

			if (!thisHashNext) {
				if (!otherhashnext) {
					// Equals
					return 0;
				} else {
					// other has more entries: this is smaller
					return -1;
				}
			} else if (!otherhashnext) {
				// this has more entries: this is bigger
				return 1;
			} else {
				Object2IntMap.Entry<String> thisNext = thisIterator.next();
				Object2IntMap.Entry<String> otherNext = otherIterator.next();

				String thisKey = thisNext.getKey();
				String otherKey = otherNext.getKey();
				int compareKey = compareKey(thisKey, otherKey);

				if (compareKey != 0) {
					return compareKey;
				} else {
					Object thisCoordinate = this.values.get(thisNext.getIntValue());
					Object otherCoordinate = o.values.get(otherNext.getIntValue());
					int valueCompare = valueComparator.compare(thisCoordinate, otherCoordinate);

					if (valueCompare != 0) {
						return valueCompare;
					}
				}
			}
		}
	}

	// We expect most key comparison to be reference comparisons as columnNames as defined once, should be
	// internalized, and keySet are identical in most cases
	// `java:S4973` is about the reference comparison, which is done on purpose to potentially skip the `.compareTo`
	@SuppressWarnings("java:S4973")
	private int compareKey(String thisKey, String otherKey) {
		if (thisKey == otherKey) {
			return 0;
		} else {
			return thisKey.compareTo(otherKey);
		}
	}

	public static class AdhocMapBuilder {
		final NavigableSet<String> keys;
		// From key original index to sortedIndex
		final int[] reordering;
		final List<Object> values;

		int added = 0;

		private AdhocMapBuilder(Collection<String> keys) {
			int size = keys.size();
			if (keys instanceof NavigableSet<String> navigableKeys) {
				this.keys = navigableKeys;
				// identity reordering
				reordering = new int[0];
			} else {
				ObjectArrayList<Object2IntMap.Entry<String>> keyToIndex = new ObjectArrayList<>();

				keys.forEach(key -> keyToIndex.add(new AbstractObject2IntMap.BasicEntry<>(key, keyToIndex.size())));

				keyToIndex.sort(Map.Entry.comparingByKey());

				this.keys = new TreeSet<>();
				this.reordering = new int[size];

				keyToIndex.forEach(e -> {
					reordering[this.keys.size()] = e.getIntValue();
					this.keys.add(e.getKey());
				});
			}
			this.values = Arrays.asList(new Object[keys.size()]);
		}

		public AdhocMapBuilder append(Object value) {
			int writeIndex;
			if (reordering.length == 0) {
				writeIndex = added;
			} else {
				writeIndex = reordering[added];
			}
			values.set(writeIndex, value);
			added++;

			return this;
		}

		public AdhocMap build() {
			// Object2IntArrayMap enables keeping the order
			Object2IntArrayMap<String> keyToIndex = new Object2IntArrayMap<>(keys.size());

			// -1 is not a valid index: it is a good default value (better than the defaultDefault 0)
			keyToIndex.defaultReturnValue(-1);

			keys.forEach(key -> keyToIndex.put(key, keyToIndex.size()));

			return new AdhocMap(keyToIndex, values);
		}
	}

	/**
	 * BEWARE of `Set` as in many cases, the order may not be the one you believe. e.g. `Set.of("a", "b")` may iterate
	 * "b" then "a".
	 *
	 * @param keys
	 *            the ordered-set of keys which will be appended
	 * @return
	 */
	public static AdhocMapBuilder builder(Collection<String> keys) {
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
