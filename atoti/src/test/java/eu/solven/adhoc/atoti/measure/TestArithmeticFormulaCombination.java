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
package eu.solven.adhoc.atoti.measure;

import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor;

import eu.solven.adhoc.engine.step.ISliceWithStep;

public class TestArithmeticFormulaCombination {

	@Test
	public void testProduct() {
		ArithmeticFormulaCombination c =
				new ArithmeticFormulaCombination(Map.of(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
						"aggregatedValue[CDS Composite Spread5Y],double[10000],*"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(0.123_456))).isEqualTo(1234.56);
		Assertions.assertThat(c.combine(slice, Arrays.asList(new Object[] { null }))).isEqualTo(null);
	}

	@Test
	public void testProduct_3operands() {
		ArithmeticFormulaCombination c =
				new ArithmeticFormulaCombination(Map.of(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
						"int[123],aggregatedValue[CDS Composite Spread5Y],double[234],*"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(34.56))).isEqualTo(123 * 34.56 * 234);
		Assertions.assertThat(c.combine(slice, Arrays.asList(new Object[] { null }))).isNull();
	}
}
