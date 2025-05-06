/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.engine;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.IAdhocFilter;

public class TestAdhocQueryStep {
	@Test
	public void testEdit() {
		CubeQueryStep step = CubeQueryStep.builder()
				// This test should customize all fields
				.measure(Aggregator.sum("c"))
				.filter(IAdhocFilter.MATCH_ALL)
				.groupBy(IAdhocGroupBy.GRAND_TOTAL)
				.customMarker("somethingCutom")
				.option(StandardQueryOptions.DEBUG)
				.option(StandardQueryOptions.EXPLAIN)
				.build();

		step.getCache().put("k", "v");

		CubeQueryStep copy = CubeQueryStep.edit(step).build();

		// Check .equals, even if some fields are not in the equals
		Assertions.assertThat(copy).isEqualTo(step);

		// Check fields not in equals
		Assertions.assertThat(copy.isExplain()).isEqualTo(step.isExplain());
		Assertions.assertThat(copy.isDebug()).isEqualTo(step.isDebug());

		// Check Cache is not copied
		Assertions.assertThat(copy.getCache()).isEmpty();
		Assertions.assertThat(step.getCache()).hasSize(1);
	}

	@Test
	public void testDebug() {
		CubeQueryStep stepNotDebug = CubeQueryStep.builder()
				.measure(Aggregator.sum("c"))
				.filter(IAdhocFilter.MATCH_ALL)
				.groupBy(IAdhocGroupBy.GRAND_TOTAL)
				.build();
		Assertions.assertThat(stepNotDebug.isDebug()).isFalse();

		CubeQueryStep stepDebug = CubeQueryStep.edit(stepNotDebug).option(StandardQueryOptions.DEBUG).build();
		Assertions.assertThat(stepDebug.isDebug()).isTrue();

		Assertions.assertThat(stepDebug).isNotEqualTo(stepNotDebug);
	}

	@Test
	public void testCustomMarker() {
		CubeQueryStep stepHasCustom = CubeQueryStep.builder()
				.measure(Aggregator.sum("c"))
				.filter(IAdhocFilter.MATCH_ALL)
				.groupBy(IAdhocGroupBy.GRAND_TOTAL)
				.customMarker(Optional.of("someCustomMarker"))
				.build();
		Assertions.assertThat((Optional) stepHasCustom.optCustomMarker()).contains("someCustomMarker");

		CubeQueryStep editKeepCustom = CubeQueryStep.edit(stepHasCustom).build();
		Assertions.assertThat((Optional) editKeepCustom.optCustomMarker()).contains("someCustomMarker");
	}
}
