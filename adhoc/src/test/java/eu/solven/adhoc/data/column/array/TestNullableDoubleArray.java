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
package eu.solven.adhoc.data.column.array;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class TestNullableDoubleArray {
	@Test
	public void testRemove() {
		NullableDoubleArray array = NullableDoubleArray.builder().build();

		array.add(12.34D);
		array.add(23.45D);
		Assertions.assertThat(array.get(0)).isEqualTo(12.34D);
		Assertions.assertThat(array.get(1)).isEqualTo(23.45D);

		Assertions.assertThat(array.removeDouble(3)).isEqualTo(0D);
		Assertions.assertThat(array.get(0)).isEqualTo(12.34D);
		Assertions.assertThat(array.get(1)).isEqualTo(23.45D);

		Assertions.assertThat(array.removeDouble(0)).isEqualTo(12.34D);
		Assertions.assertThat(array.isNull(0)).isTrue();
		Assertions.assertThat(array.get(1)).isEqualTo(23.45D);

		Assertions.assertThat(array.removeDouble(0)).isEqualTo(0D);
		Assertions.assertThat(array.isNull(0)).isTrue();
		Assertions.assertThat(array.get(1)).isEqualTo(23.45D);
	}

	@Test
	public void testAddNulThenNotNull() {
		NullableDoubleArray array = NullableDoubleArray.builder().build();

		array.addNull();
		array.add(23.45D);

		Assertions.assertThat(array.isNull(0)).isTrue();
		Assertions.assertThat(array.get(1)).isEqualTo(23.45D);
	}

	@Test
	public void testOutOfBounds() {
		NullableDoubleArray array = NullableDoubleArray.builder().build();

		array.add(12.34D);
		array.add(23.45D);

		Assertions.assertThat(array.get(-1)).isEqualTo(Double.MIN_VALUE);
		Assertions.assertThat(array.get(3)).isEqualTo(Double.MIN_VALUE);
	}

	@Test
	public void testCompact() {
		NullableDoubleArray array = NullableDoubleArray.builder().build();

		array.add(12.34D);
		array.add(23.45D);

		Assertions.assertThat(((DoubleArrayList) array.list).elements()).hasSize(10);

		array.compact();

		Assertions.assertThat(((DoubleArrayList) array.list).elements()).hasSize(2);
	}
}
