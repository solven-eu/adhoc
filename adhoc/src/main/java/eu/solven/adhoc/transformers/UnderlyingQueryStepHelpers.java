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
package eu.solven.adhoc.transformers;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import eu.solven.adhoc.coordinate.MapComparators;
import eu.solven.adhoc.dag.ISliceToValues;
import eu.solven.adhoc.slice.AdhocSliceAsMap;

public class UnderlyingQueryStepHelpers {
	protected UnderlyingQueryStepHelpers() {
		// hidden
	}

	/**
	 * @param debug
	 *            if true, the output set is ordered. This can be quite slow on large sets.
	 * @param underlyings
	 * @return the union-Set of slices
	 */
	public static Iterable<? extends AdhocSliceAsMap> distinctSlices(boolean debug,
			List<? extends ISliceToValues> underlyings) {
		Set<AdhocSliceAsMap> keySet;
		if (debug) {
			// Enforce an iteration order for debugging-purposes
			keySet = new TreeSet<>(
					Comparator.comparing(simpleSlice -> simpleSlice.getCoordinates(), MapComparators.mapComparator()));
		} else {
			keySet = new HashSet<>();
		}

		for (ISliceToValues underlying : underlyings) {
			keySet.addAll(underlying.slicesSet());
		}

		return keySet;
	}
}
