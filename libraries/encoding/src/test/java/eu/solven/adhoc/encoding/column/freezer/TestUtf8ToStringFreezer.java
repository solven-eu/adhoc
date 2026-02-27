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
package eu.solven.adhoc.encoding.column.freezer;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.bytes.Utf8ByteSlice;
import eu.solven.adhoc.encoding.column.IReadableColumn;
import eu.solven.adhoc.encoding.column.ObjectArrayColumn;

public class TestUtf8ToStringFreezer {
	Utf8ToStringFreezer freezer = new Utf8ToStringFreezer();

	@Test
	public void testFreeze_empty() {
		ObjectArrayColumn column = ObjectArrayColumn.builder().build();
		Optional<IReadableColumn> optFrozen = freezer.freeze(column, new HashMap<>());
		Assertions.assertThat(optFrozen).isEmpty();
	}

	@Test
	public void testFreeze_mixed() {
		ObjectArrayColumn column = ObjectArrayColumn.builder().build();

		column.append(Utf8ByteSlice.fromString("foo"));
		column.append(123L);
		column.append(Utf8ByteSlice.fromString("bar"));
		column.append(234L);

		Optional<IReadableColumn> optFrozen = freezer.freeze(column, new HashMap<>());
		Assertions.assertThat(optFrozen).isPresent().get().isInstanceOfSatisfying(ObjectArrayColumn.class, frozen -> {
			Assertions.assertThat((List) frozen.getAsArray()).containsExactly("foo", 123L, "bar", 234L);
		});
	}
}
