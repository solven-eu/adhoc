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
package eu.solven.adhoc.measure.lambda;

import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.ISliceWithStep;

public class TestLambdaCombination {
	ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

	@Test
	public void testCombine_sum() {
		LambdaCombination combination = new LambdaCombination(Map.of(LambdaCombination.K_LAMBDA,
				(LambdaCombination.ILambdaCombination) (s,
						values) -> values.stream().mapToLong(v -> ((Number) v).longValue()).sum()));

		Assertions.assertThat(combination.combine(slice, Arrays.asList(3L, 4L, 5L))).isEqualTo(12L);
	}

	@Test
	public void testCombine_usesSlice() {
		// Verify the slice is forwarded to the lambda
		LambdaCombination combination = new LambdaCombination(
				Map.of(LambdaCombination.K_LAMBDA, (LambdaCombination.ILambdaCombination) (s, values) -> s));

		Assertions.assertThat(combination.combine(slice, Arrays.asList(1L))).isSameAs(slice);
	}

	@Test
	public void testConstruct_missingLambda() {
		Assertions.assertThatThrownBy(() -> new LambdaCombination(Map.of()))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
