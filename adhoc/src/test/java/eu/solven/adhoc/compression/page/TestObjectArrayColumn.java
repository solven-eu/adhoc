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
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.column.LongArrayColumn;
import eu.solven.adhoc.compression.column.ObjectArrayColumn;
import eu.solven.adhoc.compression.column.freezer.SynchronousFreezingStrategy;
import eu.solven.adhoc.compression.dictionary.DictionarizedObjectColumn;

public class TestObjectArrayColumn {
	SynchronousFreezingStrategy freezer = SynchronousFreezingStrategy.builder().build();

	@Test
	public void testFromArray_empty() {
		IReadableColumn c = ObjectArrayColumn.builder().asArray(List.of()).build();
		Assertions.assertThatThrownBy(() -> c.readValue(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
	}

	@Test
	public void testFromArray_1() {
		IReadableColumn c = ObjectArrayColumn.builder().asArray(List.of("a")).build();
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
	}

	@Test
	public void testFromArray_2_mixedTypes() {
		IReadableColumn c = ObjectArrayColumn.builder().asArray(List.of("a", 123L)).build();
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
		Assertions.assertThat(c.readValue(1)).isEqualTo(123L);
	}

	@Test
	public void testFromArray_freeze_toDic() {
		List<Object> list = IntStream.range(0, 32).<Object>mapToObj(i -> i % 2 == 0 ? "a" : "b").toList();

		IReadableColumn c = ObjectArrayColumn.builder().asArray(list).build().freeze(freezer);
		Assertions.assertThat(c).isInstanceOf(DictionarizedObjectColumn.class);
		Assertions.assertThat(c.readValue(0)).isEqualTo("a");
		Assertions.assertThat(c.readValue(1)).isEqualTo("b");
	}

	@Test
	public void testFromArray_freeze_toLong() {
		List<Object> list = IntStream.range(0, 32).<Object>mapToObj(i -> (long) i).toList();

		IReadableColumn c = ObjectArrayColumn.builder().asArray(list).build().freeze(freezer);
		Assertions.assertThat(c).isInstanceOf(LongArrayColumn.class);
		Assertions.assertThat(c.readValue(0)).isEqualTo(0L);
		Assertions.assertThat(c.readValue(1)).isEqualTo(1L);
	}

}
