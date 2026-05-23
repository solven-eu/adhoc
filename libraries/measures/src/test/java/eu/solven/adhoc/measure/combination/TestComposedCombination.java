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
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.model.measure.Combinator;

public class TestComposedCombination {
	ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

	private Combinator plus(long delta) {
		return Combinator.builder().name("plus" + delta).underlying("input").lambda((s, values) -> {
			Object v = values.get(0);
			if (v == null) {
				return null;
			} else {
				return ((Number) v).longValue() + delta;
			}
		}).build();
	}

	@Test
	public void testChain_appliesInOrder() {
		ComposedCombination composed = new ComposedCombination(Map.of(ComposedCombination.K_CHAIN,
				List.of(plus(1), plus(2), plus(3)),
				StandardOperatorFactory.K_OPERATOR_FACTORY,
				StandardOperatorFactory.builder().build()));

		// 10 -> 11 -> 13 -> 16
		Assertions.assertThat(composed.combine(slice, List.of(10L))).isEqualTo(16L);
	}

	@Test
	public void testChain_propagatesNullThroughLambdas() {
		// Each lambda short-circuits on null; the chain must keep returning null.
		ComposedCombination composed = new ComposedCombination(Map.of(ComposedCombination.K_CHAIN,
				List.of(plus(1), plus(2)),
				StandardOperatorFactory.K_OPERATOR_FACTORY,
				StandardOperatorFactory.builder().build()));

		Assertions.assertThat(composed.combine(slice, Collections.singletonList(null))).isNull();
	}

	@Test
	public void testEmptyChain_throws() {
		Assertions.assertThatThrownBy(() -> new ComposedCombination(Map.of(ComposedCombination.K_CHAIN,
				List.of(),
				StandardOperatorFactory.K_OPERATOR_FACTORY,
				StandardOperatorFactory.builder().build()))).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testSingleElementChain_equivalentToTheInnerCombination() {
		// Sanity: a chain of length 1 must produce the same result as calling the inner combination directly.
		Combinator inner = plus(7);
		ComposedCombination composed = new ComposedCombination(Map.of(ComposedCombination.K_CHAIN,
				List.of(inner),
				StandardOperatorFactory.K_OPERATOR_FACTORY,
				StandardOperatorFactory.builder().build()));

		Assertions.assertThat(composed.combine(slice, List.of(100L))).isEqualTo(107L);
	}
}
