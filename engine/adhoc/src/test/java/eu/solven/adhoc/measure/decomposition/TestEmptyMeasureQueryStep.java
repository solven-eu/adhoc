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
package eu.solven.adhoc.measure.decomposition;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.engine.measure.MeasureQueryStepFactory;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.transformator.step.EmptyMeasureQueryStep;
import eu.solven.adhoc.measure.transformator.step.IMeasureQueryStep;

public class TestEmptyMeasureQueryStep {

	@Test
	public void testUnderlyings() {
		EmptyMeasure base = EmptyMeasure.builder().build();

		CubeQueryStep step = CubeQueryStep.builder().measure("m").build();
		IMeasureQueryStep transformerStep = MeasureQueryStepFactory.builder().build().makeQueryStep(step, base);

		Assertions.assertThat(transformerStep).isInstanceOf(EmptyMeasureQueryStep.class);

		// Fine on empty List
		{
			ICuboid onEmpty = transformerStep.produceOutputColumn(List.of());
			Assertions.assertThat(onEmpty.isEmpty());
		}

		Assertions.assertThatThrownBy(() -> transformerStep.produceOutputColumn(List.of(Cuboid.empty())))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Received 1");
	}
}
