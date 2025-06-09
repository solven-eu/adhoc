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
import org.mockito.Mockito;

import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.AdhocIdentity;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;

public class TestCachingOperatorFactory {
	IOperatorFactory notCaching = Mockito.mock(IOperatorFactory.class);
	CachingOperatorFactory caching = CachingOperatorFactory.builder().operatorFactory(notCaching).build();

	@Test
	public void testMakeAggregation() {
		Mockito.when(notCaching.makeAggregation(SumAggregation.KEY, Map.of())).thenReturn(new SumAggregation());

		IAggregation aggregation1 = caching.makeAggregation(SumAggregation.KEY);
		IAggregation aggregation2 = caching.makeAggregation(SumAggregation.KEY);

		Assertions.assertThat(aggregation1).isSameAs(aggregation2);
	}

	@Test
	public void testMakeCombination() {
		Mockito.when(notCaching.makeCombination(SumCombination.KEY, Map.of())).thenReturn(new SumCombination());

		ICombination aggregation1 = caching.makeCombination(SumAggregation.KEY, Map.of());
		ICombination aggregation2 = caching.makeCombination(SumAggregation.KEY, Map.of());

		Assertions.assertThat(aggregation1).isSameAs(aggregation2);
	}

	@Test
	public void testMakeDecomposition() {
		Mockito.when(notCaching.makeDecomposition(AdhocIdentity.KEY, Map.of())).thenReturn(new AdhocIdentity());

		IDecomposition aggregation1 = caching.makeDecomposition(AdhocIdentity.KEY, Map.of());
		IDecomposition aggregation2 = caching.makeDecomposition(AdhocIdentity.KEY, Map.of());

		Assertions.assertThat(aggregation1).isSameAs(aggregation2);
	}

	@Test
	public void testMakeEditor() {
		Mockito.when(notCaching.makeEditor(AdhocIdentity.KEY, Map.of())).thenReturn(new AdhocIdentity());

		IFilterEditor aggregation1 = caching.makeEditor(AdhocIdentity.KEY, Map.of());
		IFilterEditor aggregation2 = caching.makeEditor(AdhocIdentity.KEY, Map.of());

		Assertions.assertThat(aggregation1).isSameAs(aggregation2);
	}
}
