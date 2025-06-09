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

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.operator.StandardOperatorsFactory;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;

public class TestDispatchorQueryStep {
	private boolean isRelevant(Map<String, String> decompositionSLice, IAdhocFilter stepFilter) {
		Dispatchor d = Dispatchor.builder().name("d").underlying("u").build();
		CubeQueryStep cubeStep = CubeQueryStep.builder().measure("d").filter(stepFilter).build();
		DispatchorQueryStep step = new DispatchorQueryStep(d, new StandardOperatorsFactory(), cubeStep);

		return step.isRelevant(decompositionSLice);
	}

	@Test
	public void testMatchDecompositionEntry_noFilter() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of()))).isTrue();
	}

	@Test
	public void testMatchDecompositionEntry_exactMatch() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of("c", "v")))).isTrue();
	}

	@Test
	public void testMatchDecompositionEntry_exactMatch_decompositeMoreColumns() {
		Assertions.assertThat(isRelevant(Map.of("c", "v", "c2", "v2"), AndFilter.and(Map.of("c", "v")))).isTrue();
	}

	@Test
	public void testMatchDecompositionEntry_filterMore() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of("c", "v", "c2", "v2")))).isTrue();
	}

	@Test
	public void testMatchDecompositionEntry_notRelevant() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of("c", "v2")))).isFalse();
	}

	@Test
	public void testMatchDecompositionEntry_in() {
		Assertions.assertThat(isRelevant(Map.of("c", "v"), AndFilter.and(Map.of("c", Set.of("v", "v2"))))).isTrue();
	}

}
