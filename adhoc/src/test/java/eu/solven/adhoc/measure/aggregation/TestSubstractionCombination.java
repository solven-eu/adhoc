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
package eu.solven.adhoc.measure.aggregation;

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.sum.SubstractionCombination;
import eu.solven.adhoc.measure.transformator.iterator.SlicedRecordFromSlices;
import eu.solven.adhoc.util.NotYetImplementedException;

public class TestSubstractionCombination {
	ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);

	@Test
	public void testKey() {
		Assertions.assertThat(SubstractionCombination.isSubstraction("-")).isTrue();
		Assertions.assertThat(SubstractionCombination.isSubstraction("+")).isFalse();

		Assertions.assertThat(SubstractionCombination.isSubstraction(SubstractionCombination.KEY)).isTrue();
		Assertions.assertThat(SubstractionCombination.isSubstraction("foo")).isFalse();
	}

	@Test
	public void testSimpleCases_default() {
		SubstractionCombination combination = new SubstractionCombination();

		Assertions.assertThat(combination.combine(slice, Arrays.asList())).isNull();
		Assertions.assertThat(combination.combine(slice, Arrays.asList(123))).isEqualTo(123);
		Assertions.assertThat(combination.combine(slice, Arrays.asList("foo"))).isEqualTo("foo");

		Assertions.assertThat(combination.combine(slice, Arrays.asList(12, 24))).isEqualTo(0L - 12);
		Assertions.assertThat(combination.combine(slice, Arrays.asList(12D, 24D))).isEqualTo(0D - 12);

		Assertions.assertThat(combination.combine(slice, Arrays.asList(null, 234D))).isEqualTo(-234D);
		Assertions.assertThat(combination.combine(slice, Arrays.asList(123, null))).isEqualTo(123);

		Assertions.assertThat(combination.combine(slice, Arrays.asList(null, null))).isEqualTo(null);

		Assertions.assertThatThrownBy(() -> combination.combine(slice, Arrays.asList(123, "Arg")))
				.isInstanceOf(NotYetImplementedException.class);
		Assertions.assertThat(combination.combine(slice, Arrays.asList("Arg", null))).isEqualTo("Arg");

		Assertions.assertThatThrownBy(() -> combination.combine(slice, Arrays.asList(null, "Arg")))
				.isInstanceOf(NotYetImplementedException.class);
	}

	@Test
	public void testSimpleCases_fromSlicedRecord() {
		SubstractionCombination combination = new SubstractionCombination();

		Assertions
				.assertThat(IValueProvider.getValue(combination.combine(slice,
						SlicedRecordFromSlices.builder().valueProvider(IValueProvider.setValue(123)).build())))
				.isEqualTo(123L);

		Assertions.assertThat(IValueProvider.getValue(combination.combine(slice,
				SlicedRecordFromSlices.builder()
						.valueProvider(IValueProvider.setValue(123))
						.valueProvider(IValueProvider.NULL)
						.build())))
				.isEqualTo(123L);
	}
}
