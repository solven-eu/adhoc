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
package eu.solven.adhoc.dataframe.column.partitioned;

import lombok.experimental.UtilityClass;

/**
 * Utility methods for hash-based partitioning as used by {@link IPartitioned} implementations.
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public class PartitioningHelpers {

	/**
	 * Maps {@code key} to a partition index in {@code [0, nbPartitions)}.
	 *
	 * <p>
	 * {@link Math#floorMod} is used rather than {@code %} so that negative hash codes — including
	 * {@link Integer#MIN_VALUE} — always produce a non-negative index.
	 *
	 * @param key
	 *            the key to route; must not be {@code null}
	 * @param nbPartitions
	 *            the total number of partitions; must be strictly positive
	 * @return a value in {@code [0, nbPartitions)}
	 */
	public static int getPartitionIndex(Object key, int nbPartitions) {
		return Math.floorMod(key.hashCode(), nbPartitions);
	}
}
