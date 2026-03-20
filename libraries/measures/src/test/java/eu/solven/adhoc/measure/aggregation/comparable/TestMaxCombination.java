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
package eu.solven.adhoc.measure.aggregation.comparable;

import java.util.Arrays;
import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.ISliceWithStep;

public class TestMaxCombination {
	ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);
	MaxCombination combination = new MaxCombination();

	@Test
	public void testCombine_longs() {
		Assertions.assertThat(combination.combine(slice, Arrays.asList(3L, 7L, 2L))).isEqualTo(7L);
	}

	@Test
	public void testCombine_strings() {
		Assertions.assertThat(combination.combine(slice, Arrays.asList("banana", "apple", "cherry")))
				.isEqualTo("cherry");
	}

	@Test
	public void testCombine_withNulls() {
		// Nulls are filtered out; the max of the non-null values is returned
		Assertions.assertThat(combination.combine(slice, Arrays.asList(null, 5L, null))).isEqualTo(5L);
	}

	@Test
	public void testCombine_allNulls() {
		Assertions.assertThat(combination.combine(slice, Arrays.asList(null, null))).isNull();
	}

	@Test
	public void testCombine_singleValue() {
		Assertions.assertThat(combination.combine(slice, Collections.singletonList(42L))).isEqualTo(42L);
	}

	@Test
	public void testKey() {
		Assertions.assertThat(MaxCombination.KEY).isEqualTo("MAX");
	}
}
