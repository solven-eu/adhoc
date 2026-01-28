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
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;
import lombok.Builder;

/**
 * An {@link IAdhocMap} based over an underlying {@link Map}, and a mask. The mask keySet must not overlap the decorated
 * keySet.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class MaskedAdhocMap extends AbstractMap<String, Object> implements IAdhocMap {
	protected final IAdhocMap decorated;
	final Map<String, ?> mask;

	/**
	 * Holds cached entrySet(). Note that AbstractMap fields are used for keySet() and values().
	 */
	// Similar to HashMap
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	transient Set<Map.Entry<String, Object>> entrySet;

	@SuppressWarnings("PMD.LooseCoupling")
	@Override
	public int compareTo(IAdhocMap o) {
		if (o instanceof MaskedAdhocMap otherMasked) {
			if (otherMasked.mask.equals(this.mask)) {
				return this.decorated.compareTo(otherMasked.decorated);
			} else {
				return AdhocMapComparisonHelpers.compareMap(this, o);
			}
		} else {
			return AdhocMapComparisonHelpers.compareMap(this, o);
		}
	}

	@Override
	public IAdhocSlice asSlice() {
		return SliceAsMap.fromMapUnsafe(this);
	}

	@Override
	public ISliceFactory getFactory() {
		return decorated.getFactory();
	}

	// Similar to HashMap
	@SuppressWarnings({ "checkstyle.InnerAssignment", "PMD.AssignmentInOperand", "PMD.UselessParentheses" })
	@Override
	public Set<Entry<String, Object>> entrySet() {
		Set<Map.Entry<String, Object>> es;
		if ((es = entrySet) == null) {
			return (entrySet = new MaskedSliceAsMapEntrySet());
		} else {
			return es;
		}
	}

	// Called by `SliceAsMap` so it needs to be fast
	@Override
	public boolean containsKey(Object key) {
		return decorated.containsKey(key) || mask.containsKey(key);
	}

	@Override
	public Object get(Object key) {
		Object fromDecorated = decorated.get(key);
		if (fromDecorated != null) {
			return fromDecorated;
		} else {
			return mask.get(key);
		}
	}

	@Override
	public int hashCode() {
		return decorated.hashCode() + mask.hashCode();
	}

	@SuppressWarnings("PMD.LooseCoupling")
	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		} else if (o == this) {
			return true;
		} else if (o instanceof MaskedAdhocMap otherMasked) {
			if (this.mask.equals(otherMasked.mask)) {
				return this.decorated.equals(otherMasked.decorated);
			} else {
				return super.equals(o);
			}
		} else {
			return super.equals(o);
		}
	}

	/**
	 * A {@link Set} for {@link Entry} of {@link MaskedAdhocMap}.
	 * 
	 * @author Benoit Lacelle
	 * 
	 */
	public class MaskedSliceAsMapEntrySet extends AbstractSet<Map.Entry<String, Object>> {

		@Override
		public int size() {
			return decorated.size() + mask.size();
		}

		@Override
		public Iterator<Map.Entry<String, Object>> iterator() {
			Iterator<Map.Entry<String, Object>> decoratedIterator = decorated.entrySet().iterator();
			Iterator<? extends Map.Entry<String, ?>> maskIterator = mask.entrySet().iterator();

			Iterator<Map.Entry<String, Object>> maskIteratorTyped =
					Iterators.transform(maskIterator, e -> Map.entry(e.getKey(), e.getValue()));
			return Iterators.concat(decoratedIterator, maskIteratorTyped);
		}

		@Override
		public void clear() {
			throw new UnsupportedAsImmutableException();
		}

		@Override
		public boolean remove(Object o) {
			throw new UnsupportedAsImmutableException();
		}

	}

	@Override
	public IAdhocMap retainAll(Set<String> retainedColumns) {
		Map<Boolean, ImmutableSet<String>> partitioned = retainedColumns.stream()
				.collect(Collectors.partitioningBy(mask::containsKey, ImmutableSet.toImmutableSet()));

		Map<String, ?> retainedMask;
		if (partitioned.get(true).isEmpty()) {
			// no impact on mask
			retainedMask = Map.of();
		} else if (partitioned.get(true).size() == mask.size()) {
			// no impact on mask
			retainedMask = mask;
		} else {
			retainedMask = new LinkedHashMap<>(mask);
			retainedMask.keySet().retainAll(retainedColumns);
		}

		IAdhocMap retainedDecorated;
		if (partitioned.get(false).isEmpty()) {
			// no impact on decorated
			retainedDecorated = decorated;
		} else if (retainedColumns.size() == partitioned.get(false).size()) {
			// Re-use the original Collection, which is typically a Guava ImmutableCollection
			retainedDecorated = decorated.retainAll(retainedColumns);
		} else {
			retainedDecorated = decorated.retainAll(ImmutableSet.copyOf(partitioned.get(false)));
		}

		if (retainedMask.size() == mask.size()) {
			// It appears the mask contains no retained column
			retainedMask = mask;
		} else {
			retainedDecorated = decorated.retainAll(retainedColumns);
		}

		if (retainedMask.isEmpty()) {
			return retainedDecorated;
		}

		return MaskedAdhocMap.builder().decorated(retainedDecorated).mask(retainedMask).build();
	}

}
