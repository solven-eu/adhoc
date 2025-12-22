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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.IntFunction;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.experimental.UtilityClass;

/**
 * Helps implementing {@link Comparable} contract of {@link IAdhocMap}.
 * 
 * The contract is essential to compare them as NavigableMap, comparing the keySet first, then the values.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocMapComparisonHelpers {
	private static Comparator<Object> valueComparator = new ComparableElseClassComparatorV2();

	public static int compareKeySet(Iterator<String> thisKeysIterator, Iterator<String> otherKeysIterator) {
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

	// We expect most key comparison to be reference comparisons as columnNames as
	// defined once, should be
	// internalized, and keySet are identical in most cases
	// `java:S4973` is about the reference comparison, which is done on purpose to
	// potentially skip the `.compareTo`
	@SuppressWarnings({ "java:S4973", "PMD.CompareObjectsWithEquals", "PMD.UseEqualsToCompareStrings" })
	@SuppressFBWarnings("ES_COMPARING_PARAMETER_STRING_WITH_EQ")
	public static int compareKey(String thisKey, String otherKey) {
		if (thisKey == otherKey) {
			return 0;
		} else {
			return thisKey.compareTo(otherKey);
		}
	}

	// BEWARE This is a very inefficient implementation
	public static int compareMap(IAdhocMap left, IAdhocMap right) {
		NavigableMap<String, Object> leftSorted = new TreeMap<>(left);
		NavigableMap<String, Object> rightSorted = new TreeMap<>(right);

		int compareKeySet = compareKeySet(leftSorted.keySet().iterator(), rightSorted.keySet().iterator());

		if (compareKeySet != 0) {
			return compareKeySet;
		}

		return compareValues2(leftSorted.values().iterator(), rightSorted.values().iterator());
	}

	public static int compareValues(List<Object> thisOrderedValues, List<Object> otherOrderedValues) {
		int size = thisOrderedValues.size();
		if (size != otherOrderedValues.size()) {
			throw new IllegalStateException("%s != %s".formatted(size, otherOrderedValues.size()));
		}
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

	public static int compareValues(int size,
			IntFunction<Object> thisOrderedValues,
			IntFunction<Object> otherOrderedValues) {
		for (int i = 0; i < size; i++) {
			Object thisCoordinate = thisOrderedValues.apply(i);
			Object otherCoordinate = otherOrderedValues.apply(i);
			int valueCompare = valueComparator.compare(thisCoordinate, otherCoordinate);

			if (valueCompare != 0) {
				return valueCompare;
			}
		}

		return 0;
	}

	public static int compareValues2(Iterator<Object> thisOrderedValues, Iterator<Object> otherOrderedValues) {
		while (thisOrderedValues.hasNext()) {
			if (!otherOrderedValues.hasNext()) {
				throw new IllegalStateException(
						"left.next is %s while right has no next".formatted(thisOrderedValues.next()));
			}

			Object thisCoordinate = thisOrderedValues.next();
			Object otherCoordinate = otherOrderedValues.next();
			int valueCompare = valueComparator.compare(thisCoordinate, otherCoordinate);

			if (valueCompare != 0) {
				return valueCompare;
			}
		}

		if (otherOrderedValues.hasNext()) {
			throw new IllegalStateException(
					"right.next is %s while left has no next".formatted(otherOrderedValues.next()));
		}

		return 0;
	}
}
