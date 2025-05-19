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

import static org.assertj.core.api.Assertions.offset;

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
						"aggregatedValue[someMeasureName],double[10000],*"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(0.123_456))).isEqualTo(1234.56);
		Assertions.assertThat(c.combine(slice, Arrays.asList(new Object[] { null }))).isEqualTo(null);

		Assertions.assertThat(c.getUnderlyingMeasures()).containsExactly("someMeasureName");
	}

	@Test
	public void testProduct_3operands() {
		ArithmeticFormulaCombination c =
				new ArithmeticFormulaCombination(Map.of(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
						"int[123],aggregatedValue[someMeasureName],double[234],*"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(34.56))).isEqualTo(123 * 34.56 * 234);
		Assertions.assertThat(c.combine(slice, Arrays.asList(new Object[] { null }))).isNull();

		Assertions.assertThat(c.getUnderlyingMeasures()).containsExactly("someMeasureName");
	}

	// https://www.geeksforgeeks.org/evaluate-the-value-of-an-arithmetic-expression-in-reverse-polish-notation-in-java/
	@Test
	public void testComplexCase() {
		String[] array = new String[] { "10", "6", "9", "3", "+", "-11", "*", "/", "*", "17", "+", "5", "+" };
		String joined = String.join(",", array);
		ArithmeticFormulaCombination c =
				new ArithmeticFormulaCombination(Map.of(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
						joined,
						"twoOperandsPerOperator",
						true,
						"nullIfNotASingleUnderlying",
						false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat((double) c.combine(slice, Arrays.asList(34.56))).isCloseTo(21.54, offset(0.01));
		Assertions.assertThat((double) c.combine(slice, Arrays.asList(new Object[] { null })))
				.isCloseTo(21.54, offset(0.01));

		Assertions.assertThat(c.getUnderlyingMeasures()).containsExactly();
	}

	@Test
	public void testSumWithConstant() {
		ArithmeticFormulaCombination c = new ArithmeticFormulaCombination(
				Map.of(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY, "aggregatedValue[someMeasureName],int[234],+"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(123))).isEqualTo(0L + 123 + 234);
		Assertions.assertThat(c.combine(slice, Arrays.asList((Object) null))).isNull();
		Assertions.assertThat(c.combine(slice, Arrays.asList(new Object[] { null, null }))).isNull();
	}

	@Test
	public void testSumWithConstant_NotNullIfNotASingleUnderlying() {
		ArithmeticFormulaCombination c =
				new ArithmeticFormulaCombination(Map.of(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
						"aggregatedValue[someMeasureName],int[234],+",
						"nullIfNotASingleUnderlying",
						false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(123))).isEqualTo(0L + 123 + 234);
		Assertions.assertThat(c.combine(slice, Arrays.asList((Object) null))).isEqualTo(0L + 234);
	}

	@Test
	public void testSumParenthesis() {
		ArithmeticFormulaCombination c =
				new ArithmeticFormulaCombination(Map.of(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
						"(aggregatedValue[someMeasureName],aggregatedValue[otherMeasureName],int[345],+)"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(123, 234L))).isEqualTo(0L + 123 + 234 + 345);
		Assertions.assertThat(c.combine(slice, Arrays.asList(123, null))).isEqualTo(0L + 123 + 345);
		Assertions.assertThat(c.combine(slice, Arrays.asList(null, 234))).isEqualTo(0L + 234 + 345);
		Assertions.assertThat(c.combine(slice, Arrays.asList(new Object[] { null, null }))).isNull();
	}

	@Test
	public void testSubFormulas() {
		ArithmeticFormulaCombination c =
				new ArithmeticFormulaCombination(Map.of(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
						"((123,234,+),(345,456,+),*)",
						"nullIfNotASingleUnderlying",
						false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList())).isEqualTo(0L + (123 + 234) * (345 + 456));
	}

	@Test
	public void testDouble() {
		ArithmeticFormulaCombination c =
				new ArithmeticFormulaCombination(Map.of(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
						"12.34,23.45,+",
						"nullIfNotASingleUnderlying",
						false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList())).isEqualTo(0D + 12.34 + 23.45);
	}
}
