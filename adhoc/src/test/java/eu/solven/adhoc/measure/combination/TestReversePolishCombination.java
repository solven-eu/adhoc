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
package eu.solven.adhoc.measure.combination;

import static org.assertj.core.api.Assertions.offset;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.step.ISliceWithStep;

public class TestReversePolishCombination {

	@Test
	public void testProduct() {
		ReversePolishCombination c = new ReversePolishCombination(
				Map.of(ReversePolishCombination.K_NOTATION, "underlyings[someMeasureName],double[10000],*"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(0.123_456))).isEqualTo(1234.56);
		Assertions.assertThat(c.combine(slice, Arrays.asList(new Object[] { null }))).isEqualTo(null);

		Assertions.assertThat(c.getUnderlyingMeasures()).containsExactly("someMeasureName");
	}

	@Test
	public void testProduct_3operands() {
		ReversePolishCombination c = new ReversePolishCombination(
				Map.of(ReversePolishCombination.K_NOTATION, "int[123],underlyings[someMeasureName],double[234],*"));
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
		ReversePolishCombination c = new ReversePolishCombination(Map.of(ReversePolishCombination.K_NOTATION,
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
		ReversePolishCombination c = new ReversePolishCombination(
				Map.of(ReversePolishCombination.K_NOTATION, "underlyings[someMeasureName],int[234],+"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(123))).isEqualTo(0L + 123 + 234);
		Assertions.assertThat(c.combine(slice, Arrays.asList((Object) null))).isNull();
		Assertions.assertThat(c.combine(slice, Arrays.asList(new Object[] { null, null }))).isNull();
	}

	@Test
	public void testSumWithConstant_NotNullIfNotASingleUnderlying() {
		ReversePolishCombination c = new ReversePolishCombination(Map.of(ReversePolishCombination.K_NOTATION,
				"underlyings[someMeasureName],int[234],+",
				"nullIfNotASingleUnderlying",
				false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(123))).isEqualTo(0L + 123 + 234);
		Assertions.assertThat(c.combine(slice, Arrays.asList((Object) null))).isEqualTo(0L + 234);
	}

	@Test
	public void testSumParenthesis() {
		ReversePolishCombination c = new ReversePolishCombination(Map.of(ReversePolishCombination.K_NOTATION,
				"(underlyings[someMeasureName],underlyings[otherMeasureName],int[345],+)"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList(123, 234L))).isEqualTo(0L + 123 + 234 + 345);
		Assertions.assertThat(c.combine(slice, Arrays.asList(123, null))).isEqualTo(0L + 123 + 345);
		Assertions.assertThat(c.combine(slice, Arrays.asList(null, 234))).isEqualTo(0L + 234 + 345);
		Assertions.assertThat(c.combine(slice, Arrays.asList(new Object[] { null, null }))).isNull();
	}

	@Test
	public void testSubFormulas() {
		ReversePolishCombination c = new ReversePolishCombination(Map.of(ReversePolishCombination.K_NOTATION,
				"((123,234,+),(345,456,+),*)",
				"nullIfNotASingleUnderlying",
				false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList())).isEqualTo(0L + (123 + 234) * (345 + 456));
	}

	@Test
	public void testSubFormulas_null() {
		ReversePolishCombination c = new ReversePolishCombination(Map.of(ReversePolishCombination.K_NOTATION,
				"((null,234,*),(null,null,*),*)",
				"nullIfNotASingleUnderlying",
				false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList())).isNull();
	}

	@Test
	public void testDouble() {
		ReversePolishCombination c = new ReversePolishCombination(
				Map.of(ReversePolishCombination.K_NOTATION, "12.34,23.45,+", "nullIfNotASingleUnderlying", false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList())).isEqualTo(0D + 12.34 + 23.45);
	}

	@Test
	public void testMissingUnderlying() {
		ReversePolishCombination c = new ReversePolishCombination(Map.of(ReversePolishCombination.K_NOTATION,
				"underlyings[someMeasureName],underlyings[someMeasureName2],+"));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		// Not enough underlyings: failure
		Assertions.assertThatThrownBy(() -> c.combine(slice, Arrays.asList(123)))
				.isInstanceOf(IllegalArgumentException.class);

		// Too many underlyings: skip
		Assertions.assertThat(c.combine(slice, Arrays.asList(123, 234L, 345D))).isEqualTo(0L + 123 + 234);
	}

	@Test
	public void testSubstraction() {
		ReversePolishCombination c = new ReversePolishCombination(
				Map.of(ReversePolishCombination.K_NOTATION, "12.34,23.45,-", "nullIfNotASingleUnderlying", false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList())).isEqualTo(0D + 12.34 - 23.45);
	}

	@Test
	public void testPollutingWhitespaces() {
		ReversePolishCombination c = new ReversePolishCombination(Map.of(ReversePolishCombination.K_NOTATION,
				" 12.34	,\t23.45 , -\t",
				"nullIfNotASingleUnderlying",
				false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList())).isEqualTo(0D + 12.34 - 23.45);
	}

	@Test
	public void testScientificNotation_Max() {
		ReversePolishCombination c = new ReversePolishCombination(Map.of(ReversePolishCombination.K_NOTATION,
				Double.toString(Double.MAX_VALUE),
				"nullIfNotASingleUnderlying",
				false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList())).isEqualTo(Double.MAX_VALUE);
	}

	@Test
	public void testScientificNotation_MinNormal() {
		ReversePolishCombination c = new ReversePolishCombination(Map.of(ReversePolishCombination.K_NOTATION,
				Double.toString(Double.MIN_NORMAL),
				"nullIfNotASingleUnderlying",
				false));
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

		Assertions.assertThat(c.combine(slice, Arrays.asList())).isEqualTo(Double.MIN_NORMAL);
	}

	public static class ReducingCombination implements ICombination {
		@Override
		public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
			int[] intArray = (int[]) underlyingValues.get(0);
			return IntStream.of(intArray).mapToLong(i -> i).sum();
		}
	}

	@Test
	public void testCustomObject() {
		Stream.of("underlyings[someMeasureName],%s",
				"(underlyings[someMeasureName],%s)",
				"((underlyings[someMeasureName]),%s)").forEach(notation -> {
					Map<String, String> options = Map.of(ReversePolishCombination.K_NOTATION,
							notation.formatted(ReducingCombination.class.getName()));
					ReversePolishCombination c = new ReversePolishCombination(options);
					ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

					Assertions.assertThat(c.combine(slice, Arrays.asList(new int[] { 123, 234 })))
							.describedAs("notation=%s", notation)
							.isEqualTo(0L + 123 + 234);
				});
	}

	@Test
	public void testExceptionAsMeasure() {
		RuntimeException someE = new RuntimeException("someMessage");

		Stream.of("underlyings[0],underlyings[1],+").forEach(notation -> {
			Map<String, String> options = Map.of(ReversePolishCombination.K_NOTATION, notation);
			ReversePolishCombination c = new ReversePolishCombination(options);
			ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);
			Assertions.assertThat(c.combine(slice, Arrays.asList(null, someE)))
					.describedAs("notation=%s", notation)
					.isInstanceOfSatisfying(Throwable.class, t -> {
						Assertions.assertThat(t).hasSameClassAs(someE).hasMessage(someE.getMessage());
					});
		});
	}
}
