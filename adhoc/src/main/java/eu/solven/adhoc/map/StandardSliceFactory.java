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

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.objects.AbstractObject2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * In Adhoc context, we expect to build many {@link Map}-like objects, with same keySet. This is due to the fact we
 * process multiple rows with the same {@link IAdhocGroupBy}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class StandardSliceFactory implements ISliceFactory {
	// Used to prevent the following pattern: `.newMapBuilder(Set.of("a", "b")).append("a1").append("b1")` as the order
	// of the Set is not consistent with the input array
	private static final Set<Class<?>> notOrderedClasses;

	static {
		ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();

		// java.util.ImmutableCollections.Set12.Set12(E)
		builder.add(Set.of("a").getClass());
		// java.util.ImmutableCollections.SetN.SetN(E...)
		builder.add(Set.of("a", "b", "c").getClass());

		notOrderedClasses = builder.build();
	}

	final ConcurrentMap<Integer, EnrichedKeySet> keySetDictionary = new ConcurrentHashMap<>();
	final ConcurrentMap<EnrichedKeySet, Integer> keySetDictionaryReverse = new ConcurrentHashMap<>();
	final ConcurrentMap<List<String>, EnrichedNavigableSet> listToKeyset = new ConcurrentHashMap<>();

	@Default
	final ICoordinateNormalizer valueNormalizer = new StandardCoordinateNormalizer();

	public Object normalizeCoordinate(Object raw) {
		return valueNormalizer.normalizeCoordinate(raw);
	}

	/**
	 * Enable fast iteration and hashCode/equals.
	 * 
	 * Its unicity contract is mostly based on {@link List} and not {@link Set} for performance reasons. Hence, we may
	 * have multiple {@link EnrichedKeySet} with the same {@link Set}.
	 * 
	 * @author Benoit Lacelle
	 */
	@Builder
	public static class EnrichedKeySet {

		// the keySet as an unordered Set
		// Useful for quick `.equals` operations
		ImmutableSortedSet<String> keysAsSet;

		// the keySet as an ordered List
		// ImmutableList<String> keysAsList;

		public int size() {
			return keysAsSet.size();
		}

		public boolean containsAll(Collection<? extends String> keys) {
			return keysAsSet.containsAll(keys);
		}

		@Override
		public int hashCode() {
			// As this represents as keySet, the order of keys is not relevant
			return keysAsSet.hashCode();
		}

		@Override
		public boolean equals(Object other) {
			if (other == null) {
				return false;
			} else if (other == this) {
				return true;
			} else if (other instanceof EnrichedKeySet otherEnrichedKeySet) {
				return this.keysAsSet.equals(otherEnrichedKeySet.keysAsSet);
			} else {
				return false;
			}
		}

		@Override
		public String toString() {
			return keysAsSet.toString();
		}

		public int indexOf(Object key) {
			return keysAsSet.asList().indexOf(key);
		}

		public static EnrichedKeySet fromSet(NavigableSet<String> set) {
			return EnrichedKeySet.builder()
					.keysAsSet(ImmutableSortedSet.copyOf(set))
					// .keysAsList(ImmutableList.copyOf(set))
					.build();
		}

		public String getKey(int i) {
			return keysAsSet.asList().get(i);
		}
	}

	/**
	 * A {@link NavigableSet} with information to build the order in the original {@link Set}.
	 * 
	 * @author Benoit Lacelle
	 */
	@ToString
	@Builder
	public static class EnrichedNavigableSet implements Comparable<EnrichedNavigableSet> {
		final EnrichedKeySet keySet;
		final int[] reordering;

		public static EnrichedNavigableSet fromSet(Set<String> set) {
			if (set instanceof NavigableSet<String> navigableSet) {
				EnrichedKeySet keySet = EnrichedKeySet.fromSet(navigableSet);

				return EnrichedNavigableSet.builder().keySet(keySet).reordering(new int[0]).build();
			} else {
				return fromCollection(set);
			}
		}

		public static EnrichedNavigableSet fromCollection(Collection<String> distinctCollection) {
			ObjectList<Object2IntMap.Entry<String>> keyToIndex = new ObjectArrayList<>();

			distinctCollection
					.forEach(key -> keyToIndex.add(new AbstractObject2IntMap.BasicEntry<>(key, keyToIndex.size())));

			keyToIndex.sort(Map.Entry.comparingByKey());

			// TODO Could we skip the second (redundant) ordering?
			NavigableSet<String> navigableSet = ImmutableSortedSet.<String>naturalOrder()
					.addAll(keyToIndex.stream().map(Object2IntMap.Entry::getKey).toList())
					.build();

			if (distinctCollection.size() != navigableSet.size()) {
				throw new IllegalArgumentException("Duplicates in: %s".formatted(distinctCollection));
			}

			int[] reordering = keyToIndex.stream().mapToInt(Object2IntMap.Entry::getIntValue).toArray();

			EnrichedKeySet keySet = EnrichedKeySet.fromSet(navigableSet);

			return EnrichedNavigableSet.builder().keySet(keySet).reordering(reordering).build();
		}

		public int size() {
			return keySet.size();
		}

		public int indexOf(Object key) {
			return reordering[keySet.indexOf(key)];
		}

		public String getKey(int index) {
			return keySet.getKey(index);
		}

		public int unorderedIndex(int i) {
			if (reordering.length == 0) {
				return i;
			} else {
				return reordering[i];
			}
		}

		public List<String> orderedKeys() {
			return keySet.keysAsSet.asList();
		}

		@SuppressWarnings("PMD.CompareObjectsWithEquals")
		@Override
		public int compareTo(EnrichedNavigableSet keys) {
			if (this == keys) {
				return 0;
			}
			var thisKeysIterator = this.orderedKeys().iterator();
			var otherKeysIterator = keys.orderedKeys().iterator();

			// Loop until the iterator has values.
			while (true) {
				boolean thisHasNext = thisKeysIterator.hasNext();
				boolean otherHasNext = otherKeysIterator.hasNext();

				if (!thisHasNext) {
					if (!otherHasNext) {
						// Same keys
						break;
					} else {
						// other has more entries: this is smaller
						return -1;
					}
				} else if (!otherHasNext) {
					// this has more entries: this is bigger
					return 1;
				} else {
					String thisKey = thisKeysIterator.next();
					String otherKey = otherKeysIterator.next();
					int compareKey = compareKey(thisKey, otherKey);

					if (compareKey != 0) {
						return compareKey;
					}
				}
			}

			// Equivalent keySets but ordered differently
			return 0;
		}

		// We expect most key comparison to be reference comparisons as columnNames as defined once, should be
		// internalized, and keySet are identical in most cases
		// `java:S4973` is about the reference comparison, which is done on purpose to potentially skip the `.compareTo`
		@SuppressWarnings({ "java:S4973", "PMD.CompareObjectsWithEquals", "PMD.UseEqualsToCompareStrings" })
		private int compareKey(String thisKey, String otherKey) {
			if (thisKey == otherKey) {
				return 0;
			} else {
				return thisKey.compareTo(otherKey);
			}
		}
	}

	/**
	 * A {@link Map} based on {@link EnrichedNavigableSet} and values as a {@link List}.
	 * 
	 * @author Benoit Lacelle
	 */
	@SuppressWarnings("PMD.LooseCoupling")
	@Builder
	public static class MapOverLists extends AbstractMap<String, Object> implements IAdhocMap {
		private static Comparator<Object> valueComparator = new ComparableElseClassComparatorV2();

		// Holds keys, in both sorted order, and unordered order ,with the information to map from one to the other
		final EnrichedNavigableSet keys;

		final ImmutableList<Object> unorderedValues;

		/** Cache the hash code for the string */
		// Like String
		private int hash; // Default to 0

		/**
		 * Cache if the hash has been calculated as actually being zero, enabling us to avoid recalculating this.
		 */
		// Like String
		private boolean hashIsZero; // Default to false;

		/**
		 * Holds cached entrySet(). Note that AbstractMap fields are used for keySet() and values().
		 */
		// Similar to HashMap
		@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
		transient Set<Map.Entry<String, Object>> entrySet;

		protected List<Object> orderedValues() {
			return new AbstractList<>() {

				@Override
				public int size() {
					return unorderedValues.size();
				}

				@Override
				public Object get(int index) {
					return unorderedValues.get(keys.unorderedIndex(index));
				}
			};
		}

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
				return (entrySet = new AdhocEntrySet());
			} else {
				return es;
			}
		}

		@Override
		public void forEach(BiConsumer<? super String, ? super Object> action) {
			int size = unorderedValues.size();
			for (int i = 0; i < size; i++) {
				action.accept(keys.getKey(i), unorderedValues.get(keys.unorderedIndex(i)));
			}
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
			int h = hash;
			if (h == 0 && !hashIsZero) {
				int[] hashcodeHolder = new int[1];

				int size = unorderedValues.size();
				for (int i = 0; i < size; i++) {
					String key = keys.getKey(i);
					Object value = unorderedValues.get(keys.unorderedIndex(i));
					// see `Map.Entry#hashCode`
					hashcodeHolder[0] += key.hashCode() ^ value.hashCode();
				}

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
			} else if (obj instanceof MapOverLists objAsMap) {
				// hashCode is cached: while many .equals are covered by a previous hashCode check, it may be only
				// partial
				// (e.g. in a hashMap, we do .equals between instance for each hashCodes are equals modulo X)
				if (this.hashCode() != objAsMap.hashCode()) {
					return false;
				} else

				// If not equals, it is most probably spot by value than by keys
				// In other words: there is high probability of same keys and different values than different keys but
				// same values. `Objects.equals` will do a reference check
				if (!Objects.equals(orderedValues(), objAsMap.orderedValues())) {
					return false;
				} else if (!Objects.equals(keys.orderedKeys(), objAsMap.keys.orderedKeys())) {
					return false;
				} else {
					return true;
				}
			} else if (obj instanceof IAdhocMap adhocMap) {
				// BEWARE This should not happen in production, as it can be very slow
				return this.entrySet().equals(adhocMap.entrySet());
			} else if (obj instanceof Map<?, ?>) {
				// Rely on the other class .equals: BEWARE, this can lead to infinite recursive calls
				return obj.equals(this);
			} else {
				return false;
			}
		}

		@SuppressWarnings({ "checkstyle:RedundantModifier", "PMD.UnnecessaryModifier" })
		final class AdhocEntrySet extends AbstractSet<Map.Entry<String, Object>> {
			@Override
			public final int size() {
				return keys.size();
			}

			@Override
			public final void clear() {
				throw new UnsupportedOperationException("Immutable");
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
				int index = keys.indexOf(key);
				if (index < 0) {
					return false;
				}
				return Objects.equals(unorderedValues.get(index), e.getValue());
			}

			@Override
			public final boolean remove(Object o) {
				throw new UnsupportedOperationException("Immutable");
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
				for (int i = 0; i < size(); i++) {
					action.accept(entry(i));
				}
			}

			protected Entry<String, Object> entry(int i) {
				return Map.entry(keys.getKey(i), unorderedValues.get(keys.unorderedIndex(i)));
			}
		}

		// Looks a lot like NavigableMapComparator. Duplication?
		@SuppressWarnings("PMD.CompareObjectsWithEquals")
		@Override
		public int compareTo(IAdhocMap otherI) {
			if (!(otherI instanceof MapOverLists other)) {
				throw new NotYetImplementedException("other=%s".formatted(PepperLogHelper.getObjectAndClass(otherI)));
			}

			if (this == other) {
				// Typically happens when iterating along queryStep underlyings, as we often expect 2 underlyings to
				// provide
				// same sliceAsMap
				return 0;
			}

			// compare keys
			int compareKeys = keys.compareTo(other.keys);
			if (compareKeys != 0) {
				return compareKeys;
			}

			// Compare values
			List<Object> thisOrderedValues = this.orderedValues();
			int size = thisOrderedValues.size();
			List<Object> otherOrderedValues = other.orderedValues();
			assert size == otherOrderedValues.size();
			for (int i = 0; i < size; i++) {
				Object thisCoordinate = thisOrderedValues.get(i);
				Object otherCoordinate = otherOrderedValues.get(i);
				int valueCompare = valueComparator.compare(thisCoordinate, otherCoordinate);

				if (valueCompare != 0) {
					return valueCompare;
				}
			}

			return 0;
		}
	}

	/**
	 * Describe a {@link Map}-like structure by its keys and its values. The keySet and values can be zipped together
	 * (i.e. iterated concurrently).
	 * 
	 * @author Benoit Lacelle
	 */
	public interface IHasEntries {
		Collection<? extends String> getKeys();

		Collection<?> getValues();
	}

	/**
	 * A {@link IHasEntries} in which keys are provided initially, and values are received in a later phase in the same
	 * order in the initial keySet.
	 * 
	 * To be used when the keySet is known in advance.
	 * 
	 * @author Benoit Lacelle
	 */
	@Builder
	public static class MapBuilderPreKeys implements IHasEntries {
		@NonNull
		StandardSliceFactory factory;

		// Remember the ordered keys, as we expect to receive values in the same order
		@Getter
		Collection<? extends String> keys;

		ImmutableList.Builder<Object> values;

		public MapBuilderPreKeys append(Object value) {
			if (values == null) {
				values = ImmutableList.builderWithExpectedSize(keys.size());
			}
			Object v = factory.normalizeCoordinate(value);
			values.add(v);

			return this;
		}

		@Override
		public Collection<?> getValues() {
			if (values == null) {
				return ImmutableList.of();
			} else {
				return values.build();
			}
		}

		public IAdhocMap build() {
			return factory.buildMap(this);
		}
	}

	/**
	 * A {@link IHasEntries} in which keys are provided with their value.
	 * 
	 * To be used when the keySet is not known in advance.
	 * 
	 * @author Benoit Lacelle
	 */
	@Builder
	public static class MapBuilderThroughKeys implements IHasEntries {
		@NonNull
		StandardSliceFactory factory;

		// Remember the ordered keys, as we expect to receive values in the same order
		@Default
		ImmutableList.Builder<String> keys = ImmutableList.builder();

		@Default
		ImmutableList.Builder<Object> values = ImmutableList.builder();

		public MapBuilderThroughKeys put(String key, Object value) {
			keys.add(key);
			Object normalizedValue = factory.normalizeCoordinate(value);
			values.add(normalizedValue);

			return this;
		}

		@Override
		public Collection<? extends String> getKeys() {
			return keys.build();
		}

		@Override
		public Collection<?> getValues() {
			return values.build();
		}

		public IAdhocMap build() {
			return factory.buildMap(this);
		}

	}

	@Override
	public MapBuilderThroughKeys newMapBuilder() {
		return MapBuilderThroughKeys.builder().factory(this).build();
	}

	public IAdhocMap buildMap(IHasEntries hasEntries) {
		Collection<? extends String> keys = hasEntries.getKeys();
		Collection<?> values = hasEntries.getValues();

		if (keys.size() != values.size()) {
			throw new IllegalArgumentException(
					"keys size (%s) differs from values size (%s)".formatted(keys.size(), values.size()));
		}

		return MapOverLists.builder().keys(internKeyset(keys)).unorderedValues(ImmutableList.copyOf(values)).build();
	}

	@Override
	public MapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys) {
		assert !isNotOrdered(keys) : "Invalid keys: %s".formatted(PepperLogHelper.getObjectAndClass(keys));

		return MapBuilderPreKeys.builder().factory(this).keys(ImmutableList.copyOf(keys)).build();
	}

	/**
	 * This method is useful to report miss-behaving {@link Set} given we expect proper ordering: the Set may not be
	 * ordered, but one expect it to iterate consistently.
	 * 
	 * BEWARE If false, it is not guaranteed the input is ordered .
	 * 
	 * @param set
	 * @return true if the input if an ordered {@link Set}
	 */
	protected boolean isNotOrdered(Iterable<? extends String> set) {
		if (set instanceof Collection<?> c && c.isEmpty()) {
			return false;
		}

		if (notOrderedClasses.contains(set.getClass())) {
			return true;
		}

		// Assume other Set are ordered
		return false;
	}

	protected EnrichedNavigableSet internKeyset(Collection<? extends String> keys) {
		List<? extends String> keysAsList = ImmutableList.copyOf(keys);

		EnrichedNavigableSet optExisting = listToKeyset.get(keysAsList);

		if (optExisting != null) {
			return optExisting;
		} else {
			return register(keys);
		}
	}

	@SuppressWarnings("PMD.AvoidSynchronizedStatement")
	protected EnrichedNavigableSet register(Collection<? extends String> keys) {
		List<String> keysAsList = copyAsList(keys);
		// NavigableSet<String> keysAsSet = copyAsSet(keys);

		EnrichedNavigableSet navigableKeySet = listToKeyset.computeIfAbsent(keysAsList, k -> {
			return EnrichedNavigableSet.fromCollection(keysAsList);
		});

		EnrichedKeySet keySet = navigableKeySet.keySet;
		// setToKeyset.computeIfAbsent(keysAsSet, k -> EnrichedKeySet.fromSet(keysAsSet));

		synchronized (this) {
			int size = keySetDictionary.size();
			keySetDictionary.put(size, keySet);
			keySetDictionaryReverse.put(keySet, size);
		}

		return navigableKeySet;
	}

	protected List<String> copyAsList(Collection<? extends String> keys) {
		return ImmutableList.copyOf(keys);
	}

	// @Override
	// public Object getNullPlaceholder() {
	// return valueNormalizer.nullPlaceholder();
	// }
}
