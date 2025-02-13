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

import eu.solven.adhoc.measure.sum.ProductCombination;

public class TestProductCombination {
	@Test
	public void testSimpleCases_default() {
		ProductCombination combination = new ProductCombination();

		Assertions.assertThat(combination.combine(Arrays.asList())).isNull();
		Assertions.assertThat(combination.combine(Arrays.asList(123))).isEqualTo(123);

		Assertions.assertThat(combination.combine(Arrays.asList(12, 24))).isEqualTo(0L + 12 * 24);
		Assertions.assertThat(combination.combine(Arrays.asList(12D, 24D))).isEqualTo(12D * 24D);

		Assertions.assertThat(combination.combine(Arrays.asList(null, 234D))).isNull();
		Assertions.assertThat(combination.combine(Arrays.asList(123, null))).isNull();

		// BEWARE Should product returns NaN on such case?
		Assertions.assertThatThrownBy(() -> combination.combine(Arrays.asList(123, "Arg")))
				.isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThat(combination.combine(Arrays.asList(null, "Arg"))).isNull();
		Assertions.assertThat(combination.combine(Arrays.asList("Arg", null))).isNull();

		Assertions.assertThat(combination.combine(Arrays.asList(12, 0))).isEqualTo(0L);
	}

	@Test
	public void testSimpleCases_nullOperandIsNotNull() {
		ProductCombination combination = new ProductCombination(Map.of("nullOperandIsNull", false));

		Assertions.assertThat(combination.combine(Arrays.asList())).isNull();
		Assertions.assertThat(combination.combine(Arrays.asList(123))).isEqualTo(123);

		Assertions.assertThat(combination.combine(Arrays.asList(12, 24))).isEqualTo(0L + 12 * 24);
		Assertions.assertThat(combination.combine(Arrays.asList(12D, 24D))).isEqualTo(12D * 24D);

		Assertions.assertThat(combination.combine(Arrays.asList(null, 234D))).isEqualTo(234.D);
		Assertions.assertThat(combination.combine(Arrays.asList(123, null))).isEqualTo(123);

		// BEWARE Should product returns NaN on such case?
		Assertions.assertThatThrownBy(() -> combination.combine(Arrays.asList(123, "Arg")))
				.isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThat(combination.combine(Arrays.asList(null, "Arg"))).isEqualTo("Arg");
		Assertions.assertThat(combination.combine(Arrays.asList("Arg", null))).isEqualTo("Arg");

		Assertions.assertThat(combination.combine(Arrays.asList(12, 0))).isEqualTo(0L);
	}
}
