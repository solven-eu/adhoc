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
package eu.solven.adhoc.measure.combination;

import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.ISliceWithStep;

public class TestConstantCombination {
	ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

	@Test
	public void testCombine_longConstant() {
		ConstantCombination combination = new ConstantCombination(Map.of(ConstantCombination.K_CONSTANT, 42L));

		Assertions.assertThat(combination.combine(slice, Collections.emptyList())).isEqualTo(42L);
	}

	@Test
	public void testCombine_stringConstant() {
		ConstantCombination combination = new ConstantCombination(Map.of(ConstantCombination.K_CONSTANT, "hello"));

		Assertions.assertThat(combination.combine(slice, Collections.emptyList())).isEqualTo("hello");
	}

	@Test
	public void testCombine_ignoresUnderlyings() {
		ConstantCombination combination = new ConstantCombination(Map.of(ConstantCombination.K_CONSTANT, 99L));

		// Underlyings are irrelevant — always returns the constant
		Assertions.assertThat(combination.combine(slice, Collections.singletonList(1L))).isEqualTo(99L);
	}

	@Test
	public void testConstruct_missingConstant() {
		Assertions.assertThatThrownBy(() -> new ConstantCombination(Map.of()))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
