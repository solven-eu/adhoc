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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ComposedCombinationPlan.CombineStep;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.measure.sum.SumCombination;
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

	private Combinator sumCombinator(String name, String first, String second) {
		return Combinator.builder()
				.name(name)
				.underlying(first)
				.underlying(second)
				.combinationKey(SumCombination.KEY)
				.build();
	}

	private ComposedCombination compose(ComposedCombinationPlan plan) {
		return new ComposedCombination(Map.of(ComposedCombination.K_PLAN,
				plan,
				StandardOperatorFactory.K_OPERATOR_FACTORY,
				StandardOperatorFactory.builder().build()));
	}

	@Test
	public void testChain_appliesInOrder() {
		// 1 leaf (slot 0); three +k steps each reading the previous slot.
		// 10 → 11 → 13 → 16
		ComposedCombinationPlan plan = new ComposedCombinationPlan(1,
				List.of(new CombineStep(plus(1), new int[] { 0 }),
						new CombineStep(plus(2), new int[] { 1 }),
						new CombineStep(plus(3), new int[] { 2 })));

		Assertions.assertThat(compose(plan).combine(slice, List.of(10L))).isEqualTo(16L);
	}

	@Test
	public void testChain_propagatesNullThroughLambdas() {
		ComposedCombinationPlan plan = new ComposedCombinationPlan(1,
				List.of(new CombineStep(plus(1), new int[] { 0 }), new CombineStep(plus(2), new int[] { 1 })));

		Assertions.assertThat(compose(plan).combine(slice, Collections.singletonList(null))).isNull();
	}

	@Test
	public void testTree_twoBranchesSummed() {
		// 2 leaves (slots 0 and 1); two +k steps acting on each branch independently; one sum step at the root.
		// Leaves: a=10, b=20
		// Branch a: 10 → 11 → 13
		// Branch b: 20 → 22
		// Root: 13 + 22 = 35
		ComposedCombinationPlan plan = new ComposedCombinationPlan(2,
				List.of(new CombineStep(plus(1), new int[] { 0 }),
						new CombineStep(plus(2), new int[] { 2 }),
						new CombineStep(plus(2), new int[] { 1 }),
						new CombineStep(sumCombinator("sum", "left", "right"), new int[] { 3, 4 })));

		Assertions.assertThat(compose(plan).combine(slice, Arrays.asList(10L, 20L))).isEqualTo(35L);
	}

	@Test
	public void testTree_sharedLeafReferencedTwice() {
		// 1 leaf (slot 0), referenced by two distinct chains, both summed at the root. Exercises the "shared
		// underlying" case the optimizer produces when two foldable internals read the same boundary aggregator.
		// Leaf: x = 10
		// Branch a: 10 → 11 → 13 (slots 1, 2)
		// Branch b: 10 → 20 (slot 3)
		// Root: 13 + 20 = 33
		ComposedCombinationPlan plan = new ComposedCombinationPlan(1,
				List.of(new CombineStep(plus(1), new int[] { 0 }),
						new CombineStep(plus(2), new int[] { 1 }),
						new CombineStep(plus(10), new int[] { 0 }),
						new CombineStep(sumCombinator("sum", "left", "right"), new int[] { 2, 3 })));

		Assertions.assertThat(compose(plan).combine(slice, List.of(10L))).isEqualTo(33L);
	}

	@Test
	public void testSingleStepPlan_equivalentToInnerCombination() {
		ComposedCombinationPlan plan =
				new ComposedCombinationPlan(1, List.of(new CombineStep(plus(7), new int[] { 0 })));

		Assertions.assertThat(compose(plan).combine(slice, List.of(100L))).isEqualTo(107L);
	}

	@Test
	public void testPlan_rejectsForwardSlotReference() {
		// Self-reference / forward reference is an invariant violation: a step's inputs must be strictly earlier
		// slots. Detected at construction so a buggy optimizer surfaces here rather than at evaluation time.
		Assertions
				.assertThatThrownBy(
						() -> new ComposedCombinationPlan(1, List.of(new CombineStep(plus(1), new int[] { 1 }))))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("strictly earlier");
	}

	@Test
	public void testPlan_rejectsEmptyStepList() {
		Assertions.assertThatThrownBy(() -> new ComposedCombinationPlan(1, List.of()))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
