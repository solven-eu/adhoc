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
package eu.solven.adhoc.data.column;

import eu.solven.adhoc.util.AdhocUnsafe;

/**
 * Helps working with capacities through Adhoc
 * 
 * @author Benoit Lacelle
 */
public interface IAdhocCapacityConstants {
	/**
	 * Typical behavior is to start with empty structure, with zero capacity, and lazy-init the capacity on first write
	 * with {@link AdhocUnsafe#getDefaultColumnCapacity()}.
	 * 
	 * Laziness in allocating the capacity is important as we may encounter huge DAG of steps, with large empty
	 * sections.
	 */
	int ZERO_THEN_MAX = 0;

	static int capacity(int capacity) {
		if (capacity < 0) {
			throw new IllegalArgumentException("Invalid capacity: %s".formatted(capacity));
		} else if (capacity == ZERO_THEN_MAX) {
			return AdhocUnsafe.getDefaultColumnCapacity();
		} else {
			return capacity;
		}
	}
}
