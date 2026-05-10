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

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;

public class TestObjectArrayImmutableColumn {

	@Test
	public void testReadValue_byIndex() {
		ObjectArrayImmutableColumn column =
				ObjectArrayImmutableColumn.builder().asArray(ImmutableList.of("a", "b", "c")).build();

		Assertions.assertThat(column.readValue(0)).isEqualTo("a");
		Assertions.assertThat(column.readValue(2)).isEqualTo("c");
	}

	@Test
	public void testAppend_throwsUnsupported() {
		ObjectArrayImmutableColumn column =
				ObjectArrayImmutableColumn.builder().asArray(ImmutableList.of(1, 2)).build();

		Assertions.assertThatThrownBy(() -> column.append("x")).isInstanceOf(UnsupportedAsImmutableException.class);
	}

	@Test
	public void testGetAsArray_isUnmodifiable() {
		ObjectArrayImmutableColumn column =
				ObjectArrayImmutableColumn.builder().asArray(Arrays.asList(1, 2, 3)).build();

		Assertions.assertThat(column.getAsArray()).hasSize(3).first().isEqualTo(1);
		Assertions.assertThat(column.getAsArray().get(2)).isEqualTo(3);
		Assertions.assertThatThrownBy(() -> ((java.util.List) column.getAsArray()).add(4))
				.isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	public void testDefaultBuilder_yieldsEmptyColumn() {
		ObjectArrayImmutableColumn column = ObjectArrayImmutableColumn.builder().build();

		Assertions.assertThat(column.getAsArray()).isEmpty();
	}

	@Test
	public void testToString_delegatesToObjectArrayColumn() {
		ObjectArrayImmutableColumn column =
				ObjectArrayImmutableColumn.builder().asArray(ImmutableList.of("a", "b")).build();

		// Same shape as the mutable variant — just exercise the path.
		Assertions.assertThat(column.toString()).isEqualTo(ObjectArrayColumn.toString(ImmutableList.of("a", "b")));
	}
}
