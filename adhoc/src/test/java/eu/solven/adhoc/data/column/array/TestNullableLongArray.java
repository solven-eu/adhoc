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

import it.unimi.dsi.fastutil.longs.LongArrayList;

public class TestNullableLongArray {
	@Test
	public void testRemove() {
		NullableLongArray array = NullableLongArray.builder().build();

		array.add(123);
		array.add(234);
		Assertions.assertThat(array.get(0)).isEqualTo(123);
		Assertions.assertThat(array.get(1)).isEqualTo(234);

		Assertions.assertThat(array.removeLong(3)).isEqualTo(0);
		Assertions.assertThat(array.get(0)).isEqualTo(123);
		Assertions.assertThat(array.get(1)).isEqualTo(234);

		Assertions.assertThat(array.removeLong(0)).isEqualTo(123);
		Assertions.assertThat(array.isNull(0)).isTrue();
		Assertions.assertThat(array.get(1)).isEqualTo(234);

		Assertions.assertThat(array.removeLong(0)).isEqualTo(0);
		Assertions.assertThat(array.isNull(0)).isTrue();
		Assertions.assertThat(array.get(1)).isEqualTo(234);
	}

	@Test
	public void testAddNulThenNotNull() {
		NullableLongArray array = NullableLongArray.builder().build();

		array.addNull();
		array.add(234);

		Assertions.assertThat(array.isNull(0)).isTrue();
		Assertions.assertThat(array.get(1)).isEqualTo(234);
	}

	@Test
	public void testOutOfBounds() {
		NullableLongArray array = NullableLongArray.builder().build();

		array.add(123);
		array.add(234);

		Assertions.assertThat(array.get(-1)).isEqualTo(Long.MIN_VALUE);
		Assertions.assertThat(array.get(3)).isEqualTo(Long.MIN_VALUE);
	}

	@Test
	public void testCompact() {
		NullableLongArray array = NullableLongArray.builder().build();

		array.add(123);
		array.add(234);

		Assertions.assertThat(((LongArrayList) array.list).elements()).hasSize(10);

		array.compact();

		Assertions.assertThat(((LongArrayList) array.list).elements()).hasSize(2);
	}
}
