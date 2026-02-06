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
package eu.solven.adhoc.map.factory;

import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.map.AdhocMapComparisonHelpers;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * An abstract {@link IAdhocMap} based on a {@link SequencedSetLikeList} as keySet, and a List as values.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public abstract class AbstractAdhocMap implements IAdhocMap {

	@Getter
	@NonNull
	protected final ISliceFactory factory;

	// Holds keys, in both sorted order, and unordered order ,with the information
	// to map from one to the other
	@NonNull
	protected final SequencedSetLikeList sequencedKeys;

	/**
	 * Cache the hash code for the string
	 */
	// Like String
	private int hash; // Default to 0

	/**
	 * Cache if the hash has been calculated as actually being zero, enabling us to avoid recalculating this.
	 */
	// Like String
	private boolean hashIsZero; // Default to false;

	@NonNull
	final IntSupplier hashcodeSupplier;

	/**
	 * Holds cached entrySet(). Note that AbstractMap fields are used for keySet() and values().
	 */
	// Similar to HashMap
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	transient Set<Map.Entry<String, Object>> entrySet;

	// TODO Purge policy
	// TODO Compare perf with ThreadLocal
	static final Map<RetainedKeysetCacheKey, RetainedKeySet> CACHE_RETAINEDKEYS = new ConcurrentHashMap<>();

	public AbstractAdhocMap(ISliceFactory factory, SequencedSetLikeList sequencedKeys) {
		this.factory = factory;
		this.sequencedKeys = sequencedKeys;
		this.hashcodeSupplier = () -> computeHashCode(this);
	}

	/**
	 * 
	 * @param index
	 * @return the value at given index, considering keys in original order.
	 */
	// null would be represented by NullMatcher.NULL_HOLDER
	@org.jspecify.annotations.NonNull
	protected abstract Object getSequencedValueRaw(int index);

	protected Object getSequencedValue(int index) {
		return NullMatcher.unwrapNull(getSequencedValueRaw(index));
	}

	/**
	 * 
	 * @param index
	 * @return the value at given index, considering keys being sorted.
	 */
	protected abstract Object getSortedValueRaw(int index);

	@Override
	public IAdhocSlice asSlice() {
		return SliceAsMap.fromMapUnsafe(this);
	}

	// Similar to HashMap
	@SuppressWarnings({ "checkstyle.InnerAssignment", "PMD.AssignmentInOperand", "PMD.UselessParentheses" })
	@Override
	public Set<Entry<String, Object>> entrySet() {
		Set<Map.Entry<String, Object>> es;
		if ((es = entrySet) == null) {
			return (entrySet = new AdhocMapEntrySet());
		} else {
			return es;
		}
	}

	@Override
	public int size() {
		return sequencedKeys.size();
	}

	@Override
	public boolean isEmpty() {
		return sequencedKeys.isEmpty();
	}

	@Override
	public Set<String> keySet() {
		return sequencedKeys;
	}

	@Override
	public Collection<Object> values() {
		return new AbstractList<>() {

			@Override
			public int size() {
				return AbstractAdhocMap.this.size();
			}

			@Override
			public Object get(int index) {
				return getSequencedValue(index);
			}
		};
	}

	@Override
	public void forEach(BiConsumer<? super String, ? super Object> action) {
		int size = size();
		for (int i = 0; i < size; i++) {
			action.accept(sequencedKeys.getKey(i), getSequencedValue(i));
		}
	}

	@Override
	public Object get(Object key) {
		int index = sequencedKeys.indexOf(key);
		if (index < 0) {
			// key is unknown: return null as default value
			return null;
		}

		return getSequencedValue(index);
	}

	// Called by `SliceAsMap` so it needs to be fast
	@Override
	public boolean containsKey(Object key) {
		return sequencedKeys.contains(key);
	}

	@Override
	public boolean containsValue(Object value) {
		// DESIGN This is a slow method in most cases
		return IntStream.range(0, size()).anyMatch(index -> getSequencedValue(index).equals(value));
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>
	 * Computes hash code consistent with {@link java.util.Map.Entry#hashCode()}.
	 */
	@Override
	public int hashCode() {
		// hashCode caching like String.hashCode
		if (hash == 0 && !hashIsZero) {
			int h = hashcodeSupplier.getAsInt();

			if (h == 0) {
				hashIsZero = true;
			} else {
				hash = h;
			}
		}
		return hash;
	}

	/**
	 * Standard Map .hashcode, specialized for {@link AbstractAdhocMap}.
	 * 
	 * @param map
	 * @return
	 */
	@SuppressWarnings("PMD.LooseCoupling")
	private static int computeHashCode(AbstractAdhocMap map) {
		int hashcodeHolder = 0;

		int size = map.size();
		for (int i = 0; i < size; i++) {
			String key = map.sequencedKeys.getKey(i);
			Object value = map.getSequencedValue(i);
			// see `Map.Entry#hashCode`
			hashcodeHolder += Objects.hashCode(key) ^ Objects.hashCode(value);
		}

		return hashcodeHolder;
	}

	@SuppressWarnings("PMD.LooseCoupling")
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (obj instanceof AbstractAdhocMap objAsMap) {
			// hashCode is cached: while many .equals are covered by a previous hashCode check, it may be only partial
			// (e.g. in a hashMap, we do .equals between instance for each hashCodes are equals modulo X)
			if (this.hashCode() != objAsMap.hashCode()) {
				return false;
			} else if (this.size() != objAsMap.size()) {
				return false;
			} else

			// If not equals, it is most probably spot by value than by keys
			// In other words: there is high probability of same keys and different values
			// than different keys but
			// same values. `Objects.equals` will do a reference check
			if (!equalsForInt(size(), this::getSortedValueRaw, objAsMap::getSortedValueRaw)) {
				return false;
			} else if (!Objects.equals(sequencedKeys.orderedKeys(), objAsMap.sequencedKeys.orderedKeys())) {
				return false;
			} else {
				return true;
			}
		} else if (obj instanceof IAdhocMap adhocMap) {
			// BEWARE This should not happen in production, as it can be very slow
			return this.entrySet().equals(adhocMap.entrySet());
		} else if (obj instanceof Map<?, ?> otherMap) {
			// Rely on the other class .equals: BEWARE, this can lead to infinite recursive
			// calls

			return equalsAbstractMap(this, otherMap);
		} else {
			return false;
		}
	}

	/**
	 * 
	 * @param size
	 * @param left
	 * @param right
	 * @return true if the {@link IntFunction} returns equals value for index from 0 to size (excluded)
	 */
	protected static boolean equalsForInt(int size, IntFunction<Object> left, IntFunction<Object> right) {
		for (int i = 0; i < size; i++) {
			if (!Objects.equals(left.apply(i), right.apply(i))) {
				return false;
			}
		}

		return true;
	}

	// Directly copied from AbstractMap.equals
	private static boolean equalsAbstractMap(Map<?, ?> thisMap, Map<?, ?> otherMap) {
		// Similar to AbstractMap.equals
		if (thisMap.size() != otherMap.size()) {
			return false;
		}

		try {
			for (Map.Entry<?, ?> e : thisMap.entrySet()) {
				Object key = e.getKey();
				Object value = e.getValue();
				if (value == null) {
					if (!(otherMap.get(key) == null && otherMap.containsKey(key))) {
						return false;
					}
				} else {
					if (!value.equals(otherMap.get(key))) {
						return false;
					}
				}
			}
		} catch (ClassCastException | NullPointerException unused) {
			return false;
		}
		return true;
	}

	/**
	 * Standard EntrySet for {@link IAdhocMap}
	 */
	@SuppressWarnings({ "checkstyle:RedundantModifier", "PMD.UnnecessaryModifier" })
	public final class AdhocMapEntrySet extends AbstractSet<Map.Entry<String, Object>> {
		@Override
		public final int size() {
			return sequencedKeys.size();
		}

		@Override
		public final void clear() {
			throw new UnsupportedAsImmutableException();
		}

		@Override
		public final boolean remove(Object o) {
			throw new UnsupportedAsImmutableException();
		}

		@Override
		public final Iterator<Map.Entry<String, Object>> iterator() {
			return new AbstractIterator<>() {
				int index = 0;

				@Override
				protected Map.Entry<String, Object> computeNext() {
					if (index < size()) {
						Map.Entry<String, Object> entry = entry(index);
						index++;
						return entry;
					}

					// There is no more entries
					return endOfData();
				}

			};
		}

		@Override
		public final boolean contains(Object o) {
			if (!(o instanceof Map.Entry<?, ?> e)) {
				return false;
			}
			Object key = e.getKey();
			int index = sequencedKeys.indexOf(key);
			if (index < 0) {
				return false;
			}
			return Objects.equals(getSequencedValue(index), e.getValue());
		}

		@Override
		public final Spliterator<Map.Entry<String, Object>> spliterator() {
			return Spliterators.spliteratorUnknownSize(iterator(),
					Spliterator.ORDERED | Spliterator.SORTED
							| Spliterator.DISTINCT
							| Spliterator.IMMUTABLE
							| Spliterator.SIZED
							| Spliterator.NONNULL);
		}

		@Override
		public final void forEach(Consumer<? super Map.Entry<String, Object>> action) {
			int size = size();
			for (int i = 0; i < size; i++) {
				action.accept(entry(i));
			}
		}

		protected Map.Entry<String, Object> entry(int index) {
			String key = sequencedKeys.getKey(index);
			Object value = getSequencedValue(index);
			return Maps.immutableEntry(key, value);
		}
	}

	// Looks a lot like NavigableMapComparator. Duplication?
	@SuppressWarnings({ "PMD.CompareObjectsWithEquals", "PMD.LooseCoupling" })
	@Override
	public int compareTo(IAdhocMap otherI) {
		if (!(otherI instanceof AbstractAdhocMap other)) {
			throw new NotYetImplementedException("other=%s".formatted(PepperLogHelper.getObjectAndClass(otherI)));
		}

		if (this == other) {
			// Typically happens when iterating along queryStep underlyings, as we often
			// expect 2 underlyings to provide same sliceAsMap
			return 0;
		}

		// compare sorted keys
		int compareKeys = sequencedKeys.compareTo(other.sequencedKeys);
		if (compareKeys != 0) {
			return compareKeys;
		}

		// Compare values
		return AdhocMapComparisonHelpers.compareValues(this.size(), this::getSortedValueRaw, other::getSortedValueRaw);
	}

	/**
	 *
	 * @param retainedColumns
	 *            a Set of columns to retains
	 * @param sequencedKeys
	 */
	// BEWARE We store the mask 9as a Map) as it has higher change to be sameRef while `mask.KeySet` may be changed
	private record RetainedKeysetCacheKey(Set<String> retainedColumns, SequencedSetLikeList sequencedKeys) {
	}

	/**
	 * Provides relevant information to help implementing {@link IAdhocMap#retainAll(Set)}}
	 */
	@Value
	@Builder
	public static class RetainedKeySet {
		// retains keys
		@NonNull
		SequencedSetLikeList keys;

		// sequenced indexes, from retained Map, to underlying/original Map
		@NonNull
		int[] sequencedIndexes;

		// excluded keys. Typically used to compute hashCode incrementally
		@NonNull
		int[] excludedIndexes;
	}

	protected RetainedKeySet retainKeyset(Set<String> retainedColumns) {
		return CACHE_RETAINEDKEYS.computeIfAbsent(new RetainedKeysetCacheKey(retainedColumns, sequencedKeys), k -> {
			Set<String> intersection;
			if (sequencedKeys.containsAll(retainedColumns)) {
				// In most cases, we retain a subSet
				intersection = retainedColumns;
			} else {
				// `Sets.intersection` necessarily creates a new Set
				intersection = Sets.intersection(sequencedKeys, retainedColumns);
			}
			SequencedSetLikeList retainedKeyset = factory.internKeyset(intersection);

			List<String> sequencedKeysAsList = this.sequencedKeys.asList();
			int[] sequencedIndexes = retainedKeyset.stream().mapToInt(retainedColumn -> {
				int originalIndex = sequencedKeysAsList.indexOf(retainedColumn);

				if (originalIndex < 0) {
					// Throw is retaining a missing column: it does not follow resilient behavior of standard
					// `.retainAll`
					// but it may help having good performances.
					throw new IllegalArgumentException(
							"Missing %s amongst %s".formatted(retainedColumn, sequencedKeys));
				}

				return originalIndex;
			}).toArray();

			int[] excludedIndexes = sequencedKeysAsList.stream()
					.filter(existingColumn -> !retainedColumns.contains(existingColumn))
					.mapToInt(rejectedColumn -> {
						int originalIndex = sequencedKeysAsList.indexOf(rejectedColumn);

						if (originalIndex < 0) {
							// Throw is retaining a missing column: it does not follow resilient behavior of standard
							// `.retainAll`
							// but it may help having good performances.
							throw new IllegalArgumentException(
									"Missing %s amongst %s".formatted(rejectedColumn, sequencedKeys));
						}

						return originalIndex;

					})
					.toArray();

			return RetainedKeySet.builder()
					.keys(retainedKeyset)
					.sequencedIndexes(sequencedIndexes)
					.excludedIndexes(excludedIndexes)
					.build();
		});

	}

	@Override
	public IAdhocMap retainAll(Set<String> columns) {
		throw new NotYetImplementedException("TODO");
	}

	@Override
	public Object put(String key, Object value) {
		throw new UnsupportedAsImmutableException("Immutable");
	}

	@Override
	public Object remove(Object key) {
		throw new UnsupportedAsImmutableException("Immutable");
	}

	@Override
	public void putAll(Map<? extends String, ?> m) {
		throw new UnsupportedAsImmutableException("Immutable");
	}

	@Override
	public void clear() {
		throw new UnsupportedAsImmutableException("Immutable");
	}
}