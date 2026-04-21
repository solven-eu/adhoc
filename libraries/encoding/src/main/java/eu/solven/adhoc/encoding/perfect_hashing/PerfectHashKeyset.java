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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;

import eu.solven.adhoc.util.immutable.IImmutable;
import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;
import lombok.Getter;
import lombok.NonNull;

/**
 * Immutable, ordered set of {@link String} keys with O(1) {@code indexOf} via a precomputed {@link IHasIndexOf}
 * (typically a {@link SimplePerfectHash}).
 *
 * It is meant to be created once and reused as the structural part of many {@link PerfectHashMap} instances sharing the
 * same keys: the perfect-hash table is computed only once, and {@link PerfectHashMap#keySet()} returns this same
 * instance (when the map is fully populated).
 *
 * Implements {@link SequencedSet} directly (without going through {@link java.util.AbstractSet}) so that every read
 * path ({@link #contains}, {@link #iterator}, {@link #equals}, {@link #hashCode}, …) walks the underlying
 * {@code keyFields} {@link ImmutableList} by index instead of going through generic iterator-based defaults.
 *
 * @author Benoit Lacelle
 */
@ThreadSafe
public final class PerfectHashKeyset implements SequencedSet<String>, IHasIndexOf<String>, IImmutable {

	@Getter
	@NonNull
	final ImmutableList<String> keyFields;

	@NonNull
	final IHasIndexOf<String> hash;

	protected PerfectHashKeyset(ImmutableList<String> keyFields, IHasIndexOf<String> hash) {
		this.keyFields = keyFields;
		this.hash = hash;
	}

	/**
	 * Builds a {@link PerfectHashKeyset} relying on a {@link SimplePerfectHash} for the {@code key->index} mapping.
	 *
	 * @param keyFields
	 *            the ordered, distinct keys
	 * @return a reusable keyset
	 */
	// LooseCoupling: returning the concrete PerfectHashKeyset is intentional, callers chain #indexOf / #getKeyFields.
	@SuppressWarnings("PMD.LooseCoupling")
	public static PerfectHashKeyset of(List<String> keyFields) {
		ImmutableList<String> immutableKeys = ImmutableList.copyOf(keyFields);
		IHasIndexOf<String> hash = SimplePerfectHash.make(immutableKeys);
		return new PerfectHashKeyset(immutableKeys, hash);
	}

	/**
	 * @return the index of the given key, or -1 if the key is not part of this keyset.
	 */
	@Override
	public int indexOf(String key) {
		return hash.indexOf(key);
	}

	@Override
	public int unsafeIndexOf(String key) {
		return hash.unsafeIndexOf(key);
	}

	@Override
	public int size() {
		return keyFields.size();
	}

	@Override
	public boolean isEmpty() {
		return keyFields.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		if (!(o instanceof String stringKey)) {
			return false;
		}
		return hash.indexOf(stringKey) >= 0;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object o : c) {
			if (!contains(o)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Iterator<String> iterator() {
		return keyFields.iterator();
	}

	@Override
	public Spliterator<String> spliterator() {
		return keyFields.spliterator();
	}

	@Override
	public Object[] toArray() {
		return keyFields.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return keyFields.toArray(a);
	}

	@Override
	public void forEach(Consumer<? super String> action) {
		keyFields.forEach(action);
	}

	@Override
	public String getFirst() {
		return keyFields.get(0);
	}

	@Override
	public String getLast() {
		return keyFields.get(keyFields.size() - 1);
	}

	@Override
	public SequencedSet<String> reversed() {
		// Reversed view is rare for a keyset; build eagerly into a LinkedHashSet (used only as a SequencedSet impl).
		SequencedSet<String> reversed = LinkedHashSet.newLinkedHashSet(keyFields.size());
		for (int i = keyFields.size() - 1; i >= 0; i--) {
			reversed.add(keyFields.get(i));
		}
		return Collections.unmodifiableSequencedSet(reversed);
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof Set<?> other) || other.size() != keyFields.size()) {
			return false;
		}
		try {
			return other.containsAll(keyFields);
		} catch (ClassCastException unused) {
			return false;
		}
	}

	@Override
	public int hashCode() {
		// Set contract: sum of element hashCodes.
		int hash = 0;
		int total = keyFields.size();
		for (int i = 0; i < total; i++) {
			hash += keyFields.get(i).hashCode();
		}
		return hash;
	}

	@Override
	public String toString() {
		return keyFields.toString();
	}

	// Mutating operations are unsupported: this Set is immutable.

	@Override
	public boolean add(String e) {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public boolean addAll(Collection<? extends String> c) {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public void clear() {
		throw new UnsupportedAsImmutableException();
	}

}
