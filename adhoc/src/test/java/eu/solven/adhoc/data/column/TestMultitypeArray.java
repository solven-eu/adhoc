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
package eu.solven.adhoc.data.column;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.cell.IValueProvider;

public class TestMultitypeArray {
	MultitypeArray array = MultitypeArray.builder().build();

	@Test
	public void testCrossTypes_longThenDouble() {
		array.add().onLong(123L);
		array.add().onDouble(23.45);

		Assertions.assertThat(array.size()).isEqualTo(2);
		Assertions.assertThat(IValueProvider.getValue(array.read(0))).isEqualTo(123L);
		Assertions.assertThat(IValueProvider.getValue(array.read(1))).isEqualTo(23.45D);

		// TODO Should we rather migrate everything to double?
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_OBJECT);
	}

	@Test
	public void testCrossTypes_doubleThenLong() {
		array.add().onDouble(23.45);
		array.add().onLong(123L);

		Assertions.assertThat(array.size()).isEqualTo(2);
		Assertions.assertThat(IValueProvider.getValue(array.read(0))).isEqualTo(23.45D);
		Assertions.assertThat(IValueProvider.getValue(array.read(1))).isEqualTo(123L);

		// TODO Should we rather migrate everything to double?
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_OBJECT);
	}
}
