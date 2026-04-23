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
package eu.solven.adhoc.measure;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.ThrowingCombination.ThrowingCombinationException;

public class TestThrowingCombination {

	ThrowingCombination combination = new ThrowingCombination();

	protected ISliceWithStep sliceWithCoordinates(Map<String, ?> coordinates) {
		ISlice slice = Mockito.mock(ISlice.class);
		// `doReturn` rather than `when(...).thenReturn(...)` to sidestep Mockito's
		// generic-capture mismatch on `Map<String, ?> getCoordinates()`.
		Mockito.doReturn(coordinates).when(slice).getCoordinates();
		ISliceWithStep sliceWithStep = Mockito.mock(ISliceWithStep.class);
		Mockito.when(sliceWithStep.getSlice()).thenReturn(slice);
		return sliceWithStep;
	}

	@Test
	public void testCombine_throwsWithSliceInMessage() {
		ISliceWithStep slice = sliceWithCoordinates(ImmutableMap.of("country", "FR", "city", "Paris"));

		Assertions.assertThatThrownBy(() -> combination.combine(slice, Arrays.asList(1, 2)))
				.isInstanceOf(ThrowingCombinationException.class)
				.hasMessageContaining("Throwing on slice=")
				.hasMessageContaining("country=FR")
				.hasMessageContaining("city=Paris");
	}

	@Test
	public void testCombine_throwsEvenWhenUnderlyingsAreEmpty() {
		ISliceWithStep slice = sliceWithCoordinates(ImmutableMap.of("k", "v"));

		Assertions.assertThatThrownBy(() -> combination.combine(slice, Collections.emptyList()))
				.isInstanceOf(ThrowingCombinationException.class);
	}

	@Test
	public void testCombine_throwsOnGrandTotalSlice() {
		// A grandTotal slice carries no coordinates. The combination must still throw —
		// the "always throws" contract is independent of the slice shape.
		ISliceWithStep slice = sliceWithCoordinates(Collections.emptyMap());

		Assertions.assertThatThrownBy(() -> combination.combine(slice, Arrays.asList(42)))
				.isInstanceOf(ThrowingCombinationException.class)
				.hasMessageContaining("Throwing on slice={}");
	}

	@Test
	public void testMakeException_messageEmbedsSlice() {
		ThrowingCombinationException e = ThrowingCombination.makeException(ImmutableMap.of("a", 1, "b", "two"));

		Assertions.assertThat(e).isInstanceOf(RuntimeException.class).hasNoCause();
		Assertions.assertThat(e.getMessage()).startsWith("Throwing on slice=").contains("a=1").contains("b=two");
	}

	@Test
	public void testException_messageAndCauseConstructor() {
		Throwable cause = new IllegalStateException("root");
		ThrowingCombinationException e = new ThrowingCombinationException("boom", cause);

		Assertions.assertThat(e).hasMessage("boom").hasCause(cause);
	}
}
