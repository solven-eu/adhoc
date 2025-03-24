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
package eu.solven.adhoc.measure.transformator.iteratotr;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.measure.transformator.iterator.SlicedRecordFromArray;

public class TestSlicedRecordFromArray {
	@Test
	public void testToString() {
		SlicedRecordFromArray sliced =
				SlicedRecordFromArray.builder().measure("a").measure(12.34).measure(new int[] { 0, 1, 23 }).build();

		Assertions.assertThat(sliced.toString()).isEqualTo("[a, 12.34, [0, 1, 23]]");
	}

	@Test
	public void testNull() {
		SlicedRecordFromArray sliced = SlicedRecordFromArray.builder().measure("a").measure(null).build();

		Assertions.assertThat(sliced.toString()).isEqualTo("[a, null]");

		Assertions.assertThat(IValueProvider.getValue(vc -> sliced.read(0, vc))).isEqualTo("a");
		Assertions.assertThat(IValueProvider.getValue(vc -> sliced.read(1, vc))).isNull();
	}
}
