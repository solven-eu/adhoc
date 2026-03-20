/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.encoding.column;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIntegerArrayColumn {

	@Test
	public void testReadValue() {
		IntegerArrayColumn col = IntegerArrayColumn.builder().asArray(new int[] { 10, 20, 30 }).build();

		Assertions.assertThat(col.readValue(0)).isEqualTo(10);
		Assertions.assertThat(col.readValue(1)).isEqualTo(20);
		Assertions.assertThat(col.readValue(2)).isEqualTo(30);
	}

	@Test
	public void testReadValue_singleElement() {
		IntegerArrayColumn col = IntegerArrayColumn.builder().asArray(new int[] { 42 }).build();

		Assertions.assertThat(col.readValue(0)).isEqualTo(42);
	}

	@Test
	public void testReadValue_outOfBounds() {
		IntegerArrayColumn col = IntegerArrayColumn.builder().asArray(new int[] { 1, 2 }).build();

		Assertions.assertThatThrownBy(() -> col.readValue(5)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
	}
}
