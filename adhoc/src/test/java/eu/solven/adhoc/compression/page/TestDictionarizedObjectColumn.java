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
package eu.solven.adhoc.compression.page;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.dictionary.DictionarizedObjectColumn;

public class TestDictionarizedObjectColumn {
	@Test
	public void testFromArray_empty() {
		IReadableColumn c = DictionarizedObjectColumn.fromArray(List.of());
		Assertions.assertThatThrownBy(() -> c.readValue(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
	}

	@Test
	public void testFromArray_1() {
		IReadableColumn c = DictionarizedObjectColumn.fromArray(List.of("a"));
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
	}

	@Test
	public void testFromArray_2() {
		IReadableColumn c = DictionarizedObjectColumn.fromArray(List.of(new String("a"), new String("a")));
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
		Assertions.assertThat(c.readValue(1)).isSameAs(c.readValue(0));
	}

	@Test
	public void testFromArray_mixed() {
		IReadableColumn c = DictionarizedObjectColumn.fromArray(List.of("a", "b", "a", "c", "b"));

		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
		Assertions.assertThat(c.readValue(1)).isEqualTo("b");
		Assertions.assertThat(c.readValue(2)).isEqualTo("a");
		Assertions.assertThat(c.readValue(3)).isEqualTo("c");
		Assertions.assertThat(c.readValue(4)).isEqualTo("b");
	}
}
