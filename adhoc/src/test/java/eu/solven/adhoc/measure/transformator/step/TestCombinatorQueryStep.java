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
package eu.solven.adhoc.measure.transformator.step;

import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingNames;

public class TestCombinatorQueryStep {

	public static class CombinationWithUnderlyings implements ICombination, IHasUnderlyingNames {

		@Override
		public List<String> getUnderlyingNames() {
			return List.of("validUnderlying");
		}
	}

	@Test
	public void testCombinationHavingUnderlyings() {
		Combinator combinator = Combinator.builder()
				.name("someName")
				.combinationKey(CombinationWithUnderlyings.class.getName())
				.underlyings(Arrays.asList("invalidUnderlying"))
				.build();

		Assertions.assertThat(combinator.getUnderlyingNames()).containsExactly("invalidUnderlying");

		ITransformatorQueryStep node = combinator.wrapNode(AdhocFactories.builder().build(),
				CubeQueryStep.builder().measure("someName").build());

		Assertions.assertThat(node.getUnderlyingSteps()).singleElement().satisfies(step -> {
			Assertions.assertThat(step.getMeasure().getName()).isEqualTo("validUnderlying");
		});
	}

	@Test
	public void testFindFirstSingleUnderlying() {
		Combinator combinator = Combinator.builder()
				.name("someName")
				.combinationKey(CoalesceCombination.KEY)
				.underlyings(Arrays.asList("otherName"))
				.build();

		ITransformatorQueryStep node = combinator.wrapNode(AdhocFactories.builder().build(),
				CubeQueryStep.builder().measure("someName").build());

		ISliceToValue underlying = Mockito.mock(ISliceToValue.class);
		ISliceToValue output = node.produceOutputColumn(Arrays.asList(underlying));

		Assertions.assertThat(output).isSameAs(underlying);
	}
}
