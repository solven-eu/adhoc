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
package eu.solven.adhoc.measure.decomposition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.primitive.IValueProvider;

public class TestLinearDecomposition {

	final Map<String, Object> options = ImmutableMap.<String, Object>builder()
			.put(LinearDecomposition.K_INPUT, "input")
			.put(LinearDecomposition.K_OUTPUT, "output")
			.put("min", 0)
			.put("max", 100)
			.build();

	final LinearDecomposition decomposition = new LinearDecomposition(options);

	private ISliceWithStep sliceWith(String column, Object value) {
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);
		ISlice adhocSlice = Mockito.mock(ISlice.class);
		Mockito.when(slice.getSlice()).thenReturn(adhocSlice);
		Mockito.when(adhocSlice.optGroupBy(Mockito.anyString())).thenReturn(Optional.empty());
		Mockito.when(adhocSlice.optGroupBy(column)).thenReturn(Optional.of(value));
		return slice;
	}

	private ISliceWithStep sliceWithout(String column) {
		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);
		ISlice adhocSlice = Mockito.mock(ISlice.class);
		Mockito.when(slice.getSlice()).thenReturn(adhocSlice);
		Mockito.when(adhocSlice.optGroupBy(column)).thenReturn(Optional.empty());
		return slice;
	}

	@Test
	public void testDecompose_noInputColumn() {
		ISliceWithStep slice = sliceWithout("input");

		List<IDecompositionEntry> result = decomposition.decompose(slice, 200);

		// When input column is absent, return the original value with empty slice coordinates
		Assertions.assertThat(result).hasSize(1);
		Assertions.assertThat(IValueProvider.getValue(result.get(0).getValue())).isEqualTo(200);
		Assertions.assertThat(result.get(0).getSlice()).isEmpty();
	}

	@Test
	public void testDecompose_inputAtMin() {
		ISliceWithStep slice = sliceWith("input", 0);

		List<IDecompositionEntry> result = decomposition.decompose(slice, 200);

		// input == min: all value written to min bucket
		Assertions.assertThat(result).hasSize(1);
		Assertions.assertThat(result.get(0).getSlice().get("output")).isEqualTo(0);
		Assertions.assertThat(IValueProvider.getValue(result.get(0).getValue())).isEqualTo(200);
	}

	@Test
	public void testDecompose_inputAtMax() {
		ISliceWithStep slice = sliceWith("input", 100);

		List<IDecompositionEntry> result = decomposition.decompose(slice, 200);

		// input == max: all value written to max bucket
		Assertions.assertThat(result).hasSize(1);
		Assertions.assertThat(result.get(0).getSlice().get("output")).isEqualTo(100);
		Assertions.assertThat(IValueProvider.getValue(result.get(0).getValue())).isEqualTo(200);
	}

	@Test
	public void testDecompose_inputMidpoint() {
		ISliceWithStep slice = sliceWith("input", 50);

		List<IDecompositionEntry> result = decomposition.decompose(slice, 200.0);

		// input=50, min=0, max=100 → 50% of 200 = 100.0 to each bucket
		Assertions.assertThat(result).hasSize(2);
		Assertions.assertThat(result.get(0).getSlice().get("output")).isEqualTo(0);
		Assertions.assertThat(result.get(1).getSlice().get("output")).isEqualTo(100);

		Object minValue = IValueProvider.getValue(result.get(0).getValue());
		Object maxValue = IValueProvider.getValue(result.get(1).getValue());
		Assertions.assertThat(minValue).isEqualTo(100.0);
		Assertions.assertThat(maxValue).isEqualTo(100.0);
	}

	@Test
	public void testDecompose_inputQuarter() {
		ISliceWithStep slice = sliceWith("input", 25);

		List<IDecompositionEntry> result = decomposition.decompose(slice, 400.0);

		// input=25, min=0, max=100 → 25% of 400 = 100.0 to min, 300.0 to max
		Assertions.assertThat(result).hasSize(2);

		Object minValue = IValueProvider.getValue(result.get(0).getValue());
		Object maxValue = IValueProvider.getValue(result.get(1).getValue());
		Assertions.assertThat(minValue).isEqualTo(100.0);
		Assertions.assertThat(maxValue).isEqualTo(300.0);
	}

	@Test
	public void testScale_inputBelowMin() {
		// When input <= min, scale returns 0 (nothing goes to min bucket)
		Object result = decomposition.scale(0, 100, -10, 200);
		Assertions.assertThat(result).isEqualTo(0D);
	}

	@Test
	public void testScale_inputAboveMax() {
		// When input >= max, scale returns the full value
		Object result = decomposition.scale(0, 100, 110, 200);
		Assertions.assertThat(result).isEqualTo(200);
	}

	@Test
	public void testScale_nonNumericInput() {
		// scale() returns NaN when input is not a Number
		Object result = decomposition.scale(0, 100, "notANumber", 200);
		Assertions.assertThat(result).isEqualTo(Double.NaN);
	}

	@Test
	public void testScale_nonNumericValue() {
		// scale() returns NaN when the value is not a Number (midpoint path)
		Object result = decomposition.scale(0, 100, 50, "notANumber");
		Assertions.assertThat(result).isEqualTo(Double.NaN);
	}

	@Test
	public void testScaleComplement_whenScaledIsNaN() {
		// scaleComplement delegates to scale; if scale returns NaN, complement is also NaN
		Object result = decomposition.scaleComplement(0, 100, "badInput", 200);
		Assertions.assertThat(result).isEqualTo(Double.NaN);
	}

	@Test
	public void testScaleComplement_whenValueIsNotNumber() {
		// scaleComplement: scaled is a Double but value is not a Number
		Object result = decomposition.scaleComplement(0, 100, 50, "notANumber");
		Assertions.assertThat(result).isEqualTo(Double.NaN);
	}

	@Test
	public void testGetCoordinates() {
		CoordinatesSample coords = decomposition.getCoordinates("output", IValueMatcher.MATCH_ALL, 10);

		Assertions.assertThat(coords.getCoordinates()).containsExactlyInAnyOrder(0, 100);
		Assertions.assertThat(coords.getEstimatedCardinality()).isEqualTo(2);
	}
}
