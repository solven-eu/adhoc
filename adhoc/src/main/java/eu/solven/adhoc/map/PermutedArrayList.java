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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;
import java.util.function.IntUnaryOperator;

import org.jspecify.annotations.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import lombok.Builder;

/**
 * A {@link List} being the permutation of an input {@link List}.
 * 
 * It is {@link RandomAccess} as we assume the underlying {@link List} is also RandomAccess.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
@Builder
public class PermutedArrayList<T> extends AbstractList<T> implements RandomAccess {
	final ImmutableList<T> unorderedValues;

	final IntUnaryOperator reordering;

	@Override
	public int size() {
		return unorderedValues.size();
	}

	@Override
	public T get(int index) {
		return unorderedValues.get(reordering.applyAsInt(index));
	}

	// Do not rely on default implementation to skip the instantiation of an
	// iterator
	// Duplicated from ImmutableList.hashCode
	@SuppressWarnings("checkstyle:MagicNumber")
	@Override
	public int hashCode() {
		int hashCode = 1;
		int n = size();
		for (int i = 0; i < n; i++) {
			hashCode = 31 * hashCode + get(i).hashCode();
		}
		return hashCode;
	}

	@SuppressWarnings("PMD.LooseCoupling")
	@Override
	public boolean equals(Object other) {
		if (other instanceof PermutedArrayList<?> otherPermuted && unorderedValues == otherPermuted.unorderedValues
				&& reordering == otherPermuted.reordering) {
			// FastTrack
			return true;
		}
		return equalsImpl(this, other);
	}

	// Duplicated from Guava Lists
	/** An implementation of {@link List#equals(Object)}. */
	@SuppressWarnings({ "PMD.LooseCoupling", "PMD.CompareObjectsWithEquals" })
	private static boolean equalsImpl(PermutedArrayList<?> thisList, @Nullable Object other) {
		if (other == checkNotNull(thisList)) {
			return true;
		}
		if (!(other instanceof List)) {
			return false;
		}
		List<?> otherList = (List<?>) other;
		int size = thisList.size();
		if (size != otherList.size()) {
			return false;
		}
		if (thisList.unorderedValues instanceof RandomAccess && otherList instanceof RandomAccess) {
			// avoid allocation and use the faster loop
			for (int i = 0; i < size; i++) {
				if (!com.google.common.base.Objects.equal(thisList.get(i), otherList.get(i))) {
					return false;
				}
			}
			return true;
		} else {
			return Iterators.elementsEqual(thisList.iterator(), otherList.iterator());
		}
	}
}
