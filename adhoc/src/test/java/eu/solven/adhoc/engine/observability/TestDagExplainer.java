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
package eu.solven.adhoc.engine.observability;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;

public class TestDagExplainer implements IAdhocTestConstants {
	EventBus eventBus = new EventBus();
	List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

	@Test
	public void testPerfLog() {
		DagExplainer dagExplainer = DagExplainer.builder().eventBus(eventBus::post).build();

		Assertions
				.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(Aggregator.countAsterisk()).build()))
				.isEqualTo("COUNT");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(k1PlusK2AsExpr).build()))
				.isEqualTo("Combinator[EXPRESSION]");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(filterK1onA1).build()))
				.isEqualTo("Filtrator");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(unfilterOnA).build()))
				.isEqualTo("Unfiltrator");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(sum_MaxK1K2ByA).build()))
				.isEqualTo("Partitionor[MAX][SUM]");

		Assertions.assertThat(dagExplainer.toString(CubeQueryStep.builder().measure(dispatchFrom0To100).build()))
				.isEqualTo("Dispatchor[linear][SUM]");

	}

	// Ensure toString() is concise not to pollute the explain
	@Test
	public void testToString() {
		DagExplainer dagExplainer = DagExplainer.builder().eventBus(eventBus::post).build();

		Assertions.assertThat(dagExplainer.toString()).isEqualTo(DagExplainer.class.getSimpleName());
	}
}
