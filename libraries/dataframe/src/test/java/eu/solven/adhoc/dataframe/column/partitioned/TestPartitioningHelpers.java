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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitioningHelpers {

	// --- basic routing ---

	@Test
	public void getPartitionIndex_inRange() {
		int nbPartitions = 8;
		// All indices must be in [0, nbPartitions)
		for (int i = 0; i < 1000; i++) {
			int idx = PartitioningHelpers.getPartitionIndex("key" + i, nbPartitions);
			Assertions.assertThat(idx).isBetween(0, nbPartitions - 1);
		}
	}

	@Test
	public void getPartitionIndex_sameKeyAlwaysSamePartition() {
		int first = PartitioningHelpers.getPartitionIndex("stable", 4);
		int second = PartitioningHelpers.getPartitionIndex("stable", 4);

		Assertions.assertThat(first).isEqualTo(second);
	}

	@Test
	public void getPartitionIndex_singlePartition_alwaysZero() {
		Assertions.assertThat(PartitioningHelpers.getPartitionIndex("anything", 1)).isEqualTo(0);
		Assertions.assertThat(PartitioningHelpers.getPartitionIndex(42, 1)).isEqualTo(0);
	}

	// --- negative hash codes must not produce negative indices ---

	@Test
	public void getPartitionIndex_negativeHashCode_nonNegativeIndex() {
		// Craft an object whose hashCode() is negative
		Object negativeHash = new Object() {
			@Override
			public int hashCode() {
				return -7;
			}
		};

		int idx = PartitioningHelpers.getPartitionIndex(negativeHash, 4);
		Assertions.assertThat(idx).isBetween(0, 3);
	}

	@Test
	public void getPartitionIndex_intMinValue_nonNegativeIndex() {
		// Integer.MIN_VALUE is the pathological case for naive `% n` (would give negative result)
		Object minHash = new Object() {
			@Override
			public int hashCode() {
				return Integer.MIN_VALUE;
			}
		};

		int idx = PartitioningHelpers.getPartitionIndex(minHash, 8);
		Assertions.assertThat(idx).isBetween(0, 7);
	}
}
