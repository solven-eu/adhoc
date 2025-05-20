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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.operator.StandardOperatorsFactory;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;

public class TestColumnatorQueryStep {

	@Test
	public void testDefaultIsRequired() {
		Columnator measure = Columnator.builder().name("measureName").column("c").build();

		Assertions.assertThat(measure.isRequired()).isTrue();
	}

	@Test
	public void testFilterMultiSelection() {
		Columnator measure = Columnator.builder().name("measureName").column("c").build();

		ColumnatorQueryStep queryStep =
				new ColumnatorQueryStep(measure, new StandardOperatorsFactory(), Mockito.mock(CubeQueryStep.class));

		Assertions.assertThat(queryStep.isMonoSelected(IAdhocFilter.MATCH_ALL, "c")).isFalse();
		// TODO should matchNone be considered mono-selected, as not multi-selected, which is generally the expected
		// semantic?
		Assertions.assertThat(queryStep.isMonoSelected(IAdhocFilter.MATCH_NONE, "c")).isFalse();

		Assertions.assertThat(queryStep.isMonoSelected(ColumnFilter.isEqualTo("c", "foo"), "c")).isTrue();
		Assertions.assertThat(queryStep.isMonoSelected(ColumnFilter.isEqualTo("d", "foo"), "c")).isFalse();

		Assertions.assertThat(queryStep.isMonoSelected(ColumnFilter.isIn("c", "foo", "bar"), "c")).isFalse();
	}
}
