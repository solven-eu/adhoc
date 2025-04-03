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

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.dag.step.ISliceWithStep;
import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.measure.transformator.iterator.SlicedRecordFromArray;

public class TestICombination {
	@Test
	public void testImplementSliceAndList() {
		ICombination sliceAndList = new ICombination() {
			@Override
			public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
				// Return a copy
				return underlyingValues.stream().toList();
			}
		};

		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);
		ISlicedRecord record = SlicedRecordFromArray.builder().measure("inputA").build();

		Object output = IValueProvider.getValue(sliceAndList.combine(slice, record));
		Assertions.assertThat(output).isEqualTo(List.of("inputA"));
	}

	@Test
	public void testImplementList() {
		ICombination sliceAndList = new ICombination() {
			@Override
			public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
				// Return a copy
				return underlyingValues.stream().toList();
			}
		};

		ISliceWithStep slice = Mockito.mock(ISliceWithStep.class);
		ISlicedRecord record = SlicedRecordFromArray.builder().measure("inputA").build();

		Object output = IValueProvider.getValue(sliceAndList.combine(slice, record));
		Assertions.assertThat(output).isEqualTo(List.of("inputA"));
	}
}
