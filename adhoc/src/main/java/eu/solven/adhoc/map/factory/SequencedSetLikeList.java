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

import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.SequencedSet;
import java.util.Set;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingSet;
import com.google.common.collect.ImmutableSortedSet;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import eu.solven.adhoc.map.AdhocMapComparisonHelpers;
import eu.solven.adhoc.util.NotYetImplementedException;
import it.unimi.dsi.fastutil.objects.AbstractObject2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.Builder;
import lombok.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A {@link SequencedSet} with information to build the order in the original {@link Set}.
 * 
 * This is NOT similar to a {@link NavigableSet}. It enables an order relatively to {@link NavigableSet}.
 * 
 * This should generally not be used as key, as two {@link SequencedSet} with different orders would be considered
 * equals. In such a case, one should rely on
 *
 * @author Benoit Lacelle
 */
public final class SequencedSetLikeList extends ForwardingSet<String>
		implements SequencedSet<String>, Comparable<SequencedSetLikeList>, ILikeList<String> {
	@NonNull
	final NavigableSetLikeList set;

	// If empty, there is no reordering
	// Else, map from the position to the sequence to the position in the navigableSet
	@NonNull
	final int[] reordering;

	final int[] reversedOrdering;

	@Builder
	private SequencedSetLikeList(NavigableSetLikeList set, int... reordering) {
		this.set = set;
		this.reordering = reordering;

		this.reversedOrdering = new int[reordering.length];
		for (int i = 0; i < reordering.length; i++) {
			reversedOrdering[reordering[i]] = i;
		}
	}

	public static SequencedSetLikeList fromSet(Set<String> set) {
		if (set instanceof NavigableSet<String> navigableSet) {
			NavigableSetLikeList setLikeList = NavigableSetLikeList.fromSet(navigableSet);

			return SequencedSetLikeList.builder().set(setLikeList).reordering(new int[0]).build();
		} else {
			return fromCollection(set);
		}
	}

	public static SequencedSetLikeList fromCollection(Collection<String> distinctCollection) {
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

		NavigableSetLikeList setLikeList = NavigableSetLikeList.fromSet(navigableSet);

		return SequencedSetLikeList.builder().set(setLikeList).reordering(reordering).build();
	}

	@Override
	protected Set<String> delegate() {
		return set;
	}

	/**
	 * @param key
	 * @return the index given the unordered keySet
	 */
	@Override
	public int indexOf(Object key) {
		int index = set.indexOf(key);
		if (index < 0) {
			return index;
		}
		return unorderedIndex(index);
	}

	@Override
	public String getKey(int index) {
		return set.getKey(reversedIndex(index));
	}

	public int unorderedIndex(int i) {
		if (reordering.length == 0) {
			return i;
		} else {
			return reordering[i];
		}
	}

	public int reversedIndex(int i) {
		if (reversedOrdering.length == 0) {
			return i;
		} else {
			return reversedOrdering[i];
		}
	}

	public List<String> orderedKeys() {
		return set.keysAsSet.asList();
	}

	// hashCode and equals are delegated by ForwardingSet
	@SuppressWarnings({ "PMD.CompareObjectsWithEquals", "PMD.OverrideBothEqualsAndHashCodeOnComparable" })
	@Override
	public int compareTo(SequencedSetLikeList keys) {
		if (this == keys) {
			return 0;
		}
		var thisKeysIterator = this.orderedKeys().iterator();
		var otherKeysIterator = keys.orderedKeys().iterator();

		return AdhocMapComparisonHelpers.compareKeySet(thisKeysIterator, otherKeysIterator);
	}

	@Override
	public Iterator<String> iterator() {
		if (reversedOrdering.length == 0) {
			return set.iterator();
		} else {
			return IntStream.of(reversedOrdering).mapToObj(i -> orderedKeys().get(i)).iterator();
		}
	}

	@Override
	public SequencedSet<String> reversed() {
		throw new NotYetImplementedException("TODO");
	}

	// Duplicated from AbstractCollection, in order to take ordering in account
	@SuppressWarnings("PMD.ConsecutiveLiteralAppends")
	@Override
	public String toString() {
		Iterator<String> it = iterator();
		if (!it.hasNext()) {
			return "[]";
		}

		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for (;;) {
			String e = it.next();
			sb.append(e);
			if (!it.hasNext()) {
				return sb.append(']').toString();
			}
			sb.append(',').append(' ');
		}
	}

	protected class SequenceSetAsList extends AbstractList<String> implements RandomAccess {

		@Override
		public int size() {
			return SequencedSetLikeList.this.size();
		}

		@Override
		public String get(int index) {
			return SequencedSetLikeList.this.getKey(index);
		}

		// from Guava ImmutableList/Lists.equalsImpl
		@Override
		public boolean equals(@Nullable Object other) {
			if (other == null) {
				return false;
			} else if (other == this) {
				return true;
			} else if (!(other instanceof List<?> otherList)) {
				return false;
			} else {
				int size = this.size();
				if (size != otherList.size()) {
					return false;
				} else if (otherList instanceof RandomAccess) {
					for(int i = 0; i < size; ++i) {
						if (!Objects.equals(this.get(i), otherList.get(i))) {
							return false;
						}
					}

					return true;
				} else {
					return Iterators.elementsEqual(this.iterator(), otherList.iterator());
				}
			}
		}

		// from ImmutableList. Improve AbstractList by not creating an iterator given this is RandomAccess
		@Override
		public int hashCode() {
			int hashCode = 1;
			int n = this.size();

			for(int i = 0; i < n; ++i) {
				Object e = this.get(i);
				hashCode = 31 * hashCode + (e==null ? 0 : e.hashCode()) ;
			}

			return hashCode;
		}
	}

	public List<String> asList() {
		return new SequenceSetAsList();
	}

}