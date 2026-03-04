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
package eu.solven.adhoc.measure.operator;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.decomposition.LinearDecomposition;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumNotNaNAggregation;
import eu.solven.adhoc.util.AdhocIdentity;

public class TestCompositeOperatorFactory {
	@Test
	public void testFallbackOnError() {
		IOperatorFactory operatorFactory = CompositeOperatorFactory.builder()
				.operatorFactory(new StandardOperatorFactory())
				.operatorFactory(DummyOperatorFactory.builder().build())
				.build();

		Assertions.assertThat(operatorFactory.makeAggregation(SumNotNaNAggregation.KEY))
				.isInstanceOf(SumNotNaNAggregation.class);
		Assertions.assertThat(operatorFactory.makeAggregation("unknownKey")).isInstanceOf(SumAggregation.class);

		Assertions.assertThat(operatorFactory.makeCombination(DivideCombination.KEY, Map.of()))
				.isInstanceOf(DivideCombination.class);
		Assertions.assertThat(operatorFactory.makeCombination("unknownKey", Map.of()))
				.isInstanceOf(CoalesceCombination.class);

		Assertions.assertThat(operatorFactory.makeDecomposition(LinearDecomposition.KEY, Map.of()))
				.isInstanceOf(LinearDecomposition.class);
		Assertions.assertThat(operatorFactory.makeDecomposition("unknownKey", Map.of()))
				.isInstanceOf(AdhocIdentity.class);

		Assertions
				.assertThat(operatorFactory.makeEditor(SimpleFilterEditor.KEY,
						Map.of(SimpleFilterEditor.P_SHIFTED, Map.of("country", "France"))))
				.isInstanceOf(SimpleFilterEditor.class);
		Assertions.assertThat(operatorFactory.makeEditor("unknownKey", Map.of())).isInstanceOf(AdhocIdentity.class);
	}
}
