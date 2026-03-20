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
package eu.solven.adhoc.engine.measure;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.IMeasureQueryStep;
import lombok.RequiredArgsConstructor;

public class TestMeasureQueryStepFactory {
	public static class SomeStep_DefaultConstructor implements IMeasureQueryStep {

		@Override
		public List<CubeQueryStep> getUnderlyingSteps() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
			throw new UnsupportedOperationException();
		}

	}

	@Test
	public void testMake_emptyCtor() {
		MeasureQueryStepFactory factory = MeasureQueryStepFactory.builder().build();

		CubeQueryStep queryStep = CubeQueryStep.builder().measure("m").build();
		IHasUnderlyingMeasures measure = Mockito.mock(IHasUnderlyingMeasures.class);
		Mockito.when(measure.queryStepClass()).thenReturn(SomeStep_DefaultConstructor.class.getName());

		IMeasureQueryStep step = factory.makeQueryStep(queryStep, measure);
		Assertions.assertThat(step).isInstanceOf(SomeStep_DefaultConstructor.class);
	}

	@RequiredArgsConstructor
	public static class SomeStep_GenericMeasure implements IMeasureQueryStep {
		final IHasUnderlyingMeasures m;

		@Override
		public List<CubeQueryStep> getUnderlyingSteps() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
			throw new UnsupportedOperationException();
		}

	}

	@Test
	public void testMake_genericMeasure() {
		MeasureQueryStepFactory factory = MeasureQueryStepFactory.builder().build();

		CubeQueryStep queryStep = CubeQueryStep.builder().measure("m").build();
		IHasUnderlyingMeasures measure = Mockito.mock(IHasUnderlyingMeasures.class);
		Mockito.when(measure.queryStepClass()).thenReturn(SomeStep_GenericMeasure.class.getName());

		IMeasureQueryStep step = factory.makeQueryStep(queryStep, measure);
		Assertions.assertThat(step).isInstanceOf(SomeStep_GenericMeasure.class);
	}
}
