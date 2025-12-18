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

import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ForwardingNavigableSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Enable fast iteration and hashCode/equals.
 * <p>
 * Its unicity contract is mostly based on {@link List} and not {@link Set} for performance reasons. Hence, we may have
 * multiple {@link NavigableSetLikeList} with the same {@link Set}.
 *
 * @author Benoit Lacelle
 */
@Builder
public final class NavigableSetLikeList extends ForwardingNavigableSet<String> implements ILikeList<String> {
	// the keySet as an ordered Set
	// Useful for quick `.equals` operations
	@Getter(AccessLevel.PROTECTED)
	@NonNull
	final ImmutableSortedSet<String> keysAsSet;

	// cache lazily the keySet as a hashSet for faster `.containsKey`
	final Supplier<ImmutableSet<String>> keysAsHashSet = Suppliers.memoize(() -> ImmutableSet.copyOf(getKeysAsSet()));

	@Override
	protected NavigableSet<String> delegate() {
		return keysAsSet;
	}

	@Override
	public int hashCode() {
		return keysAsSet.hashCode();
	}

	// Behave like a Set
	@Override
	public boolean equals(Object other) {
		if (other == null) {
			return false;
		} else if (other == this) {
			return true;
		} else if (other instanceof NavigableSetLikeList otherEnrichedKeySet) {
			return this.keysAsSet.equals(otherEnrichedKeySet.keysAsSet);
		} else if (other instanceof Set<?> otherSet) {
			return this.keysAsSet.equals(otherSet);
		} else {
			return false;
		}
	}

	/**
	 * @param key
	 * @return the index in the ordered keySet
	 */
	@Override
	public int indexOf(Object key) {
		if (key instanceof String s) {
			return Collections.binarySearch(keysAsSet.asList(), s);
		} else {
			return -1;
		}
	}

	@Override
	public String getKey(int i) {
		return keysAsSet.asList().get(i);
	}

	public static NavigableSetLikeList fromSet(Set<String> set) {
		return NavigableSetLikeList.builder().keysAsSet(ImmutableSortedSet.copyOf(set)).build();
	}

}