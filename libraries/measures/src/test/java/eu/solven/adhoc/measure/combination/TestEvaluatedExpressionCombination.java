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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.transformator.IHasCombinationKey;

public class TestEvaluatedExpressionCombination {
	ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

	// ── combine ────────────────────────────────────────────────────────────

	@Test
	public void testCombine_sum() {
		EvaluatedExpressionCombination combination = new EvaluatedExpressionCombination("a + b", List.of("a", "b"));

		Assertions.assertThat(combination.combine(slice, Arrays.asList(3, 4))).isEqualTo(7L);
	}

	@Test
	public void testCombine_difference() {
		EvaluatedExpressionCombination combination = new EvaluatedExpressionCombination("a - b", List.of("a", "b"));

		Assertions.assertThat(combination.combine(slice, Arrays.asList(10, 3))).isEqualTo(7L);
	}

	@Test
	public void testCombine_product() {
		EvaluatedExpressionCombination combination = new EvaluatedExpressionCombination("a * b", List.of("a", "b"));

		Assertions.assertThat(combination.combine(slice, Arrays.asList(3, 4))).isEqualTo(12L);
	}

	@Test
	public void testCombine_byIndex() {
		// Access underlyings via positional `underlyings[i]` syntax
		EvaluatedExpressionCombination combination =
				new EvaluatedExpressionCombination("underlyings[0] + underlyings[1]", List.of("a", "b"));

		Assertions.assertThat(combination.combine(slice, Arrays.asList(3, 4))).isEqualTo(7L);
	}

	@Test
	public void testCombine_doubleResult() {
		EvaluatedExpressionCombination combination = new EvaluatedExpressionCombination("a / b", List.of("a", "b"));

		// 1/4 is not an exact long, so the result stays as a double
		Assertions.assertThat(combination.combine(slice, Arrays.asList(1, 4))).isEqualTo(0.25D);
	}

	@Test
	public void testCombine_exactLong() {
		EvaluatedExpressionCombination combination = new EvaluatedExpressionCombination("a / b", List.of("a", "b"));

		// 8/2 is an exact long
		Assertions.assertThat(combination.combine(slice, Arrays.asList(8, 2))).isEqualTo(4L);
	}

	@Test
	public void testCombine_invalidExpression() {
		EvaluatedExpressionCombination combination = new EvaluatedExpressionCombination("(a + b", List.of("a", "b"));

		Assertions.assertThatThrownBy(() -> combination.combine(slice, Arrays.asList(1, 2)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("expression=`(a + b`");
	}

	// ── checkSanity ────────────────────────────────────────────────────────

	@Test
	public void testCheckSanity_valid() {
		EvaluatedExpressionCombination combination = new EvaluatedExpressionCombination("a + b", List.of("a", "b"));

		// Should not throw
		combination.checkSanity();
	}

	// ── parse ──────────────────────────────────────────────────────────────

	@Test
	public void testParse() {
		EvaluatedExpressionCombination combination =
				EvaluatedExpressionCombination.parse(ImmutableMap.<String, Object>builder()
						.put(EvaluatedExpressionCombination.K_EXPRESSION, "a + b")
						.put(IHasCombinationKey.KEY_UNDERLYING_NAMES, List.of("a", "b"))
						.build());

		Assertions.assertThat(combination.combine(slice, Arrays.asList(3, 4))).isEqualTo(7L);
	}

	@Test
	public void testParse_missingExpression() {
		Assertions
				.assertThatThrownBy(() -> EvaluatedExpressionCombination
						.parse(Map.of(IHasCombinationKey.KEY_UNDERLYING_NAMES, List.of("a"))))
				.isInstanceOf(IllegalArgumentException.class);
	}

	// ── KEY constant ───────────────────────────────────────────────────────

	@Test
	public void testKey() {
		Assertions.assertThat(EvaluatedExpressionCombination.KEY).isEqualTo("EXPRESSION");
	}
}
