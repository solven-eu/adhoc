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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;

import eu.solven.adhoc.util.immutable.IImmutable;
import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;

/**
 * An immutable {@link Map} keyed by {@link String}, backed by a shared {@link PerfectHashKeyset} (an ordered set of
 * keys + a {@link SimplePerfectHash}) and an {@link ImmutableList} of values aligned with the keyset.
 *
 * Compared to {@link com.google.common.collect.ImmutableMap}, this is meant to be used when many maps share the same
 * key set: the keyset (and especially the perfect-hash table) is computed only once and reused, while each row only
 * pays for an {@link ImmutableList} of size {@code keyFields.size()}.
 *
 * Slots may carry the {@link #ABSENT} sentinel (instead of a real value) to encode `not in this map`, working around
 * {@link ImmutableList}'s rejection of nulls.
 *
 * Implements {@link Map} directly (instead of extending {@link java.util.AbstractMap}) so that every read path
 * ({@link #get}, {@link #containsKey}, {@link #containsValue}, {@link #forEach}, {@link #keySet}, {@link #values},
 * {@link #entrySet}, {@link #equals}, {@link #hashCode}) walks the underlying values list by index instead of going
 * through an iterator over {@link Map.Entry} allocations.
 *
 * @param <V>
 *            the values type
 * @author Benoit Lacelle
 */
@ThreadSafe
@SuppressWarnings({ "unchecked", "PMD.LooseCoupling", "PMD.GodClass", "PMD.AvoidFieldNameMatchingMethodName" })
public final class PerfectHashMap<V> implements Map<String, V>, IImmutable {

	/**
	 * Sentinel marking a slot as `not in this map`. Public so callers building an {@link ImmutableList} of values
	 * directly (instead of going through {@link PerfectHashMap.Builder}) can mark missing entries.
	 */
	public static final Object ABSENT = new Object() {
		@Override
		public String toString() {
			return "ABSENT";
		}
	};

	final PerfectHashKeyset keys;
	final ImmutableList<?> values;
	// Pre-computed once at construction so size() / isEmpty() / keySet() are O(1).
	final int nbPresent;

	// package-private: instances are constructed via PerfectHashMap#newBuilder / #wrap to enforce size invariants.
	PerfectHashMap(PerfectHashKeyset keys, ImmutableList<?> values) {
		this(keys, values, nbAbsent(values));
	}

	private static int nbAbsent(ImmutableList<?> values) {
		int absent = 0;
		int total = values.size();
		for (int i = 0; i < total; i++) {
			if (values.get(i) == ABSENT) {
				absent++;
			}
		}
		return absent;
	}

	PerfectHashMap(PerfectHashKeyset keys, ImmutableList<?> values, int absent) {
		if (keys.size() != values.size()) {
			throw new IllegalArgumentException("Inconsistent keys=%s values=%s".formatted(keys, values));
		}
		this.keys = keys;
		this.values = values;
		this.nbPresent = values.size() - absent;
	}

	/**
	 * @return a new {@link Builder} producing a {@link PerfectHashMap} backed by the given keyset.
	 */
	public static <V> Builder<V> newBuilder(PerfectHashKeyset keys) {
		return new Builder<>(keys);
	}

	public static <V> BuilderRandom<V> newBuilderRandom(PerfectHashKeyset keys) {
		return new BuilderRandom<>(keys);
	}

	/**
	 * Wraps a values list (aligned with {@code keys.getKeyFields()}) into an immutable {@link PerfectHashMap}.
	 *
	 * Use {@link #ABSENT} to mark an index as `not present` (so that {@link Map#get} and {@link Map#containsKey} behave
	 * as if the entry was never put). Other slots must be non-null since {@link ImmutableList} disallows nulls.
	 *
	 * @param keys
	 *            the shared keyset
	 * @param values
	 *            an {@link ImmutableList} of size {@code keys.size()}.
	 * @return an immutable Map view over the given values.
	 */
	public static <V> PerfectHashMap<V> wrap(PerfectHashKeyset keys, ImmutableList<?> values) {
		if (values.size() != keys.size()) {
			throw new IllegalArgumentException(
					"values.size=%s but keyFields.size=%s".formatted(values.size(), keys.size()));
		}
		return new PerfectHashMap<>(keys, values);
	}

	/**
	 * @return the {@link PerfectHashKeyset} (keys + perfect hash) shared by this map.
	 */
	public PerfectHashKeyset getKeyset() {
		return keys;
	}

	@Override
	public int size() {
		return nbPresent;
	}

	@Override
	public boolean isEmpty() {
		return nbPresent == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		if (!(key instanceof String stringKey)) {
			return false;
		}
		int index = keys.indexOf(stringKey);
		if (index < 0) {
			return false;
		}
		return values.get(index) != ABSENT;
	}

	@Override
	public boolean containsValue(Object value) {
		int total = values.size();
		for (int i = 0; i < total; i++) {
			Object slot = values.get(i);
			if (slot != ABSENT && Objects.equals(slot, value)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public V get(Object key) {
		if (!(key instanceof String stringKey)) {
			return null;
		}
		int index = keys.indexOf(stringKey);
		if (index < 0) {
			return null;
		}
		Object value = values.get(index);
		if (value == ABSENT) {
			return null;
		}
		return (V) value;
	}

	@Override
	public V getOrDefault(Object key, V defaultValue) {
		if (!(key instanceof String stringKey)) {
			return defaultValue;
		}
		int index = keys.indexOf(stringKey);
		if (index < 0) {
			return defaultValue;
		}
		Object value = values.get(index);
		if (value == ABSENT) {
			return defaultValue;
		}
		return (V) value;
	}

	@Override
	public void forEach(BiConsumer<? super String, ? super V> action) {
		// Iterate by index, skipping ABSENT slots — avoids allocating Map.Entry instances.
		List<String> keyFields = keys.keyFields;
		int total = values.size();
		for (int i = 0; i < total; i++) {
			Object value = values.get(i);
			if (value != ABSENT) {
				action.accept(keyFields.get(i), (V) value);
			}
		}
	}

	@Override
	public Set<String> keySet() {
		// Fast path: when the map is fully populated, the keyset matches the schema's keyset exactly. Return the
		// shared, precomputed PerfectHashKeyset (which is itself a SequencedSet) instead of building a fresh view.
		if (nbPresent == values.size()) {
			return keys;
		}
		return new SparseKeySet();
	}

	@Override
	public Collection<V> values() {
		return new ValuesCollection();
	}

	@Override
	public Set<Map.Entry<String, V>> entrySet() {
		return new EntrySet();
	}

	// Mutating operations are unsupported: this Map is immutable.

	@Override
	public V put(String key, V value) {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public void putAll(Map<? extends String, ? extends V> m) {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public void clear() {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof Map<?, ?> other) || other.size() != nbPresent) {
			return false;
		}
		List<String> keyFields = keys.keyFields;
		int total = values.size();
		try {
			for (int i = 0; i < total; i++) {
				Object value = values.get(i);
				if (value == ABSENT) {
					continue;
				}
				String key = keyFields.get(i);
				Object otherValue = other.get(key);
				if (otherValue == null && !other.containsKey(key)) {
					return false;
				}
				if (!Objects.equals(value, otherValue)) {
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
		int hash = 0;
		List<String> keyFields = keys.keyFields;
		int total = values.size();
		for (int i = 0; i < total; i++) {
			Object value = values.get(i);
			if (value != ABSENT) {
				hash += keyFields.get(i).hashCode() ^ value.hashCode();
			}
		}
		return hash;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder().append('{');
		List<String> keyFields = keys.keyFields;
		int total = values.size();
		boolean first = true;
		for (int i = 0; i < total; i++) {
			Object value = values.get(i);
			if (value == ABSENT) {
				continue;
			}
			if (!first) {
				sb.append(", ");
			}
			sb.append(keyFields.get(i)).append('=').append(value);
			first = false;
		}
		return sb.append('}').toString();
	}

	private final class SparseKeySet extends AbstractSet<String> {
		@Override
		public int size() {
			return nbPresent;
		}

		@Override
		public boolean contains(Object o) {
			return containsKey(o);
		}

		@Override
		public Iterator<String> iterator() {
			return new AbstractIterator<>() {
				int index;

				@Override
				protected String computeNext() {
					int total = values.size();
					while (index < total) {
						Object value = values.get(index);
						if (value != ABSENT) {
							String key = keys.keyFields.get(index);
							index++;
							return key;
						}
						index++;
					}
					return endOfData();
				}
			};
		}
	}

	private final class ValuesCollection extends AbstractCollection<V> {
		@Override
		public int size() {
			return nbPresent;
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
					int total = values.size();
					while (index < total) {
						Object value = values.get(index);
						if (value != ABSENT) {
							index++;
							return (V) value;
						}
						index++;
					}
					return endOfData();
				}
			};
		}
	}

	private final class EntrySet extends AbstractSet<Map.Entry<String, V>> {
		@Override
		public int size() {
			return nbPresent;
		}

		@Override
		public Iterator<Map.Entry<String, V>> iterator() {
			return new AbstractIterator<>() {
				int index;

				@Override
				protected Map.Entry<String, V> computeNext() {
					int total = values.size();
					while (index < total) {
						Object value = values.get(index);
						if (value != ABSENT) {
							String key = keys.keyFields.get(index);
							Map.Entry<String, V> entry = Map.entry(key, (V) value);
							index++;
							return entry;
						}
						index++;
					}
					return endOfData();
				}
			};
		}
	}

	/**
	 * Incremental builder of a {@link PerfectHashMap}.
	 *
	 * Slots not explicitly {@link #put} are treated as `not present` (using {@link PerfectHashMap#ABSENT} as sentinel)
	 * and will not show up in the resulting {@link Map}.
	 *
	 * @param <V>
	 *            the values type
	 */
	public static final class Builder<V> {
		final AtomicInteger nbAbsent = new AtomicInteger();
		final PerfectHashKeyset keys;
		final ImmutableList.Builder<Object> values;

		protected Builder(PerfectHashKeyset keys) {
			this.keys = keys;
			this.values = ImmutableList.builderWithExpectedSize(keys.size());
		}

		public void append(Object value) {
			if (value == null) {
				values.add(ABSENT);
				nbAbsent.incrementAndGet();
			} else {
				values.add(value);
			}
		}

		/**
		 * @return an immutable {@link PerfectHashMap} reflecting the current state of this builder.
		 */
		public PerfectHashMap<V> build() {
			// Wrap the array as ImmutableList without copying. ABSENT acts as a non-null sentinel so that
			// ImmutableList.copyOf does not reject `not put` slots.
			return new PerfectHashMap<>(keys, values.build(), nbAbsent.get());
		}
	}

	/**
	 * Random-access variant of {@link Builder}: callers set values via {@link #put(String, Object)}, addressed by key.
	 *
	 * @param <V>
	 *            the values type
	 */
	public static final class BuilderRandom<V> {
		final PerfectHashKeyset keys;
		final Object[] values;

		protected BuilderRandom(PerfectHashKeyset keys) {
			this.keys = keys;
			this.values = new Object[keys.size()];
			Arrays.fill(this.values, ABSENT);
		}

		/**
		 * Sets the value associated with the given key.
		 *
		 * @param key
		 *            must be one of the {@link PerfectHashKeyset#getKeyFields()}.
		 * @param value
		 *            must be non-null.
		 * @return this builder
		 */
		public BuilderRandom<V> put(String key, V value) {
			Objects.requireNonNull(value, () -> "value is null for key=" + key);
			int index = keys.indexOf(key);
			if (index < 0) {
				throw new IllegalArgumentException("Unknown key=%s (known=%s)".formatted(key, keys.keyFields));
			}
			values[index] = value;
			return this;
		}

		/**
		 * @return an immutable {@link PerfectHashMap} reflecting the current state of this builder.
		 */
		public PerfectHashMap<V> build() {
			// Wrap the array as ImmutableList without copying. ABSENT acts as a non-null sentinel so that
			// ImmutableList.copyOf does not reject `not put` slots.
			ImmutableList<Object> immutableValues = ImmutableList.copyOf(values);
			return new PerfectHashMap<>(keys, immutableValues);
		}
	}
}
