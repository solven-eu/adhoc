/**
 * The MIT License
 * Copyright (c) 2023-2024 Benoit Chatain Lacelle - SOLVEN
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

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Objects;

//import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Enables comparing {@link Comparable} of different {@link Class} in the same {@link NavigableSet}.
 *
 * @author Benoit Lacelle
 *
 */
// @SuppressFBWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
public class ComparableElseClassComparatorV2 implements Comparator<Object> {
	static final Comparator<Object> NULLS_HIGH = nullsHigh();

	final Comparator<Object> nullComparator;

	/**
	 * Return a {@link Comparable} adapter which accepts null values and sorts them higher than non-null values.
	 * 
	 * @see Comparator#nullsLast(Comparator)
	 */
	// Duplicated from Spring Comparators
	@SuppressWarnings("unchecked")
	static <T> Comparator<T> nullsHigh() {
		return nullsHigh((Comparator<T>) Comparator.naturalOrder());
	}

	/**
	 * Return a decorator for the given comparator which accepts null values and sorts them higher than non-null values.
	 * 
	 * @see Comparator#nullsLast(Comparator)
	 */
	// Duplicated from Spring Comparators
	static <T> Comparator<T> nullsHigh(Comparator<T> comparator) {
		return Comparator.nullsLast(comparator);
	}

	/**
	 * By default, `null` is considered greater than anything else
	 */
	public ComparableElseClassComparatorV2() {
		this(NULLS_HIGH);
	}

	public ComparableElseClassComparatorV2(Comparator<Object> nullComparator) {
		this.nullComparator = nullComparator;
	}

	@Override
	@SuppressWarnings("PMD.UnnecessaryCast")
	public int compare(Object l, Object r) {
		return doCompare(nullComparator, l, r);
	}

	public static int doCompare(Comparator<Object> nullComparator, Object l, Object r) {
		if (l == null || r == null) {
			return Objects.compare(l, r, nullComparator);
		}
		Class<?> c1 = l.getClass();
		Class<?> c2 = r.getClass();

		// Class objects are singleton: they can be compared by ref
		if (c1 != c2) {
			// Objects have different classes: we compare by className
			// This prevents classes being able to be compared with compare (e.g. `Child extends Parent implements
			// Comparable<Parent>`)
			return c1.getName().compareTo(c2.getName());
		}

		if (Comparable.class.isAssignableFrom(c1)) {
			// Objects has same class and are Comparable: the unchecked cast should be fine
			return ((Comparable) l).compareTo(r);
		} else {
			// Same class but not comparable
			// BEWARE Should we warn on this?
			return l.toString().compareTo(r.toString());
		}
	}

}