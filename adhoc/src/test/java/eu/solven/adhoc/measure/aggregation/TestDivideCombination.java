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
package eu.solven.adhoc.measure.aggregation;

import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.sum.DivideCombination;

public class TestDivideCombination {
	@Test
	public void testSimpleCases_default() {
		DivideCombination combination = new DivideCombination();

		Assertions.assertThatThrownBy(() -> combination.combine(Arrays.asList()))
				.isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThatThrownBy(() -> combination.combine(Arrays.asList(123)))
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThat(combination.combine(Arrays.asList(12, 24))).isEqualTo(0.5D);
		Assertions.assertThat(combination.combine(Arrays.asList(12D, 24D))).isEqualTo(0.5D);

		Assertions.assertThat(combination.combine(Arrays.asList(null, 234D))).isEqualTo(null);
		Assertions.assertThat(combination.combine(Arrays.asList(132, null))).isEqualTo(Double.NaN);

		Assertions.assertThat(combination.combine(Arrays.asList(132, "Arg"))).isEqualTo(Double.NaN);
		Assertions.assertThat(combination.combine(Arrays.asList(null, "Arg"))).isEqualTo(Double.NaN);
		Assertions.assertThat(combination.combine(Arrays.asList("Arg", null))).isEqualTo(Double.NaN);

		Assertions.assertThat(combination.combine(Arrays.asList(12, 0))).isEqualTo(Double.POSITIVE_INFINITY);
	}

	@Test
	public void testSimpleCases_nullNumeratorIsZero() {
		DivideCombination combination = new DivideCombination(Map.of("nullNumeratorIsZero", true));

		Assertions.assertThatThrownBy(() -> combination.combine(Arrays.asList()))
				.isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThatThrownBy(() -> combination.combine(Arrays.asList(123)))
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThat(combination.combine(Arrays.asList(12, 24))).isEqualTo(0.5D);
		Assertions.assertThat(combination.combine(Arrays.asList(12D, 24D))).isEqualTo(0.5D);

		Assertions.assertThat(combination.combine(Arrays.asList(null, 234D))).isEqualTo(0.0D);
		Assertions.assertThat(combination.combine(Arrays.asList(132, null))).isEqualTo(Double.NaN);

		Assertions.assertThat(combination.combine(Arrays.asList(132, "Arg"))).isEqualTo(Double.NaN);
		Assertions.assertThat(combination.combine(Arrays.asList(null, "Arg"))).isEqualTo(Double.NaN);
		Assertions.assertThat(combination.combine(Arrays.asList("Arg", null))).isEqualTo(Double.NaN);

		Assertions.assertThat(combination.combine(Arrays.asList(12, 0))).isEqualTo(Double.POSITIVE_INFINITY);
	}
}
