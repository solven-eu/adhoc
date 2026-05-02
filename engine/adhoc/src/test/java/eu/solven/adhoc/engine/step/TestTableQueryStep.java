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
package eu.solven.adhoc.engine.step;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestTableQueryStep {
	@Test
	public void testEquals() {
		EqualsVerifier.forClass(TableQueryStep.class).withIgnoredFields("id", "cache").verify();
	}

	@Test
	public void edit() {
		TableQueryStep step = TableQueryStep.builder()
				// This test should customize all fields
				.aggregator(Mockito.mock(Aggregator.class))
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(IGroupBy.GRAND_TOTAL)
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
}
