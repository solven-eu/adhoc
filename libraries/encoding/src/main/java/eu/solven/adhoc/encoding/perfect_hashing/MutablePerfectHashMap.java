/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.encoding.perfect_hashing;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import com.google.common.collect.AbstractIterator;

/**
 * A mutable {@link Map} backed by a {@link SimplePerfectHash} that is rebuilt on every structural mutation (new key via
 * {@link #put}, {@link #remove}, {@link #clear}). Existing-key {@link #put} is O(1) — only the values slot is
 * overwritten, the hash table is untouched.
 *
 * Intended for callers with many {@link #get} calls and very few {@link #put} calls, e.g.
 * {@code ThreadLocalAppendableTable#keyToPage} where a handful of column combinations are registered up front and then
 * each data row reads the current page via {@link #get}.
 *
 * <b>Not thread-safe.</b> Typical usage is from a single thread (e.g. inside a {@link ThreadLocal}); share an instance
 * across threads only under external synchronization.
 *
 * <b>Null values are rejected</b>, for parity with {@link PerfectHashMap} and so that {@code get} returning
 * {@code null} unambiguously means `key is absent`.
 *
 * @param <K>
 *            the key type — must have stable {@code hashCode}/{@code equals}
 * @param <V>
 *            the value type
 * @author Benoit Lacelle
 */
@SuppressWarnings("unchecked")
public class MutablePerfectHashMap<K, V> implements Map<K, V> {

	private static final Object[] EMPTY = new Object[0];

	// Insertion-ordered list of keys; valuesArr[i] is the value associated with keyList.get(i).
	private final List<K> keyList = new ArrayList<>();
	// Parallel to keyList; grows/shrinks together. Kept as Object[] (instead of ArrayList<V>) to reuse Arrays.copyOf.
	private Object[] valuesArr = EMPTY;

	// Rebuilt on every structural mutation. SimplePerfectHash#indexOf returns the position in the List passed to
	// make(),
	// which by construction equals the index of the key in keyList — hence the index into `valuesArr`.
	private IHasIndexOf<K> hash = SimplePerfectHash.<K>make(List.of());

	/**
	 * Creates an empty mutable map.
	 */
	public MutablePerfectHashMap() {
		// Explicit no-arg constructor so callers don't rely on Lombok defaults.
	}

	@Override
	public int size() {
		return keyList.size();
	}

	@Override
	public boolean isEmpty() {
		return keyList.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return hash.indexOf((K) key) >= 0;
	}

	@Override
	public boolean containsValue(Object value) {
		int n = keyList.size();
		for (int i = 0; i < n; i++) {
			if (Objects.equals(valuesArr[i], value)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public V get(Object key) {
		int idx = hash.indexOf((K) key);
		if (idx < 0) {
			return null;
		}
		return (V) valuesArr[idx];
	}

	@Override
	public V getOrDefault(Object key, V defaultValue) {
		int idx = hash.indexOf((K) key);
		if (idx < 0) {
			return defaultValue;
		}
		return (V) valuesArr[idx];
	}

	@Override
	public V put(K key, V value) {
		Objects.requireNonNull(value, () -> "value is null for key=" + key);
		int idx = hash.indexOf(key);
		if (idx >= 0) {
			// Existing key: overwrite the slot in-place, no rehash needed.
			V old = (V) valuesArr[idx];
			valuesArr[idx] = value;
			return old;
		}
		// New key: append to keyList / valuesArr and rebuild the perfect hash.
		keyList.add(key);
		valuesArr = Arrays.copyOf(valuesArr, keyList.size());
		valuesArr[keyList.size() - 1] = value;
		hash = SimplePerfectHash.make(keyList);
		return null;
	}

	@Override
	public V remove(Object key) {
		int idx = hash.indexOf((K) key);
		if (idx < 0) {
			return null;
		}
		V old = (V) valuesArr[idx];
		keyList.remove(idx);
		// Shift valuesArr down to keep the parallel-array invariant before rebuilding the hash.
		Object[] next = new Object[keyList.size()];
		System.arraycopy(valuesArr, 0, next, 0, idx);
		System.arraycopy(valuesArr, idx + 1, next, idx, keyList.size() - idx);
		valuesArr = next;
		hash = SimplePerfectHash.make(keyList);
		return old;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		// Naive: delegates to #put per entry. Each new key triggers a rehash — acceptable since putAll is not on the
		// hot path for this map (see class Javadoc).
		m.forEach(this::put);
	}

	@Override
	public void clear() {
		keyList.clear();
		valuesArr = EMPTY;
		hash = SimplePerfectHash.<K>make(List.of());
	}

	@Override
	public void forEach(BiConsumer<? super K, ? super V> action) {
		int n = keyList.size();
		for (int i = 0; i < n; i++) {
			action.accept(keyList.get(i), (V) valuesArr[i]);
		}
	}

	@Override
	public Set<K> keySet() {
		return new KeySet();
	}

	@Override
	public Collection<V> values() {
		return new ValuesCollection();
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet();
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof Map<?, ?> other) || other.size() != keyList.size()) {
			return false;
		}
		try {
			int n = keyList.size();
			for (int i = 0; i < n; i++) {
				K key = keyList.get(i);
				Object otherValue = other.get(key);
				if (otherValue == null && !other.containsKey(key)) {
					return false;
				}
				if (!Objects.equals(valuesArr[i], otherValue)) {
					return false;
				}
			}
		} catch (ClassCastException unused) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		// Map contract: sum of Map.Entry#hashCode of every entry.
		int h = 0;
		int n = keyList.size();
		for (int i = 0; i < n; i++) {
			K key = keyList.get(i);
			int keyHash;
			if (key == null) {
				keyHash = 0;
			} else {
				keyHash = key.hashCode();
			}
			h += keyHash ^ valuesArr[i].hashCode();
		}
		return h;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder().append('{');
		int n = keyList.size();
		for (int i = 0; i < n; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(keyList.get(i)).append('=').append(valuesArr[i]);
		}
		return sb.append('}').toString();
	}

	private final class KeySet extends AbstractSet<K> {
		@Override
		public int size() {
			return keyList.size();
		}

		@Override
		public boolean contains(Object o) {
			return containsKey(o);
		}

		@Override
		public Iterator<K> iterator() {
			return Collections.unmodifiableList(keyList).iterator();
		}
	}

	private final class ValuesCollection extends AbstractCollection<V> {
		@Override
		public int size() {
			return keyList.size();
		}

		@Override
		public boolean contains(Object o) {
			return containsValue(o);
		}

		@Override
		public Iterator<V> iterator() {
			return new AbstractIterator<>() {
				int index;

				@Override
				protected V computeNext() {
					if (index < keyList.size()) {
						V v = (V) valuesArr[index];
						index++;
						return v;
					}
					return endOfData();
				}
			};
		}
	}

	private final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
		@Override
		public int size() {
			return keyList.size();
		}

		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new AbstractIterator<>() {
				int index;

				@Override
				protected Map.Entry<K, V> computeNext() {
					if (index < keyList.size()) {
						Map.Entry<K, V> entry = Map.entry(keyList.get(index), (V) valuesArr[index]);
						index++;
						return entry;
					}
					return endOfData();
				}
			};
		}
	}

}
