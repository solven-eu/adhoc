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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.bytes.Utf8ByteSlice;
import eu.solven.adhoc.encoding.column.IReadableColumn;
import eu.solven.adhoc.encoding.column.ObjectArrayColumn;

/**
 * Regression: a low-cardinality column of {@link Utf8ByteSlice} (e.g. an Arrow VARCHAR column) is short-circuited by
 * {@link eu.solven.adhoc.encoding.dictionary.DistinctFreezer} before {@link Utf8ToStringFreezer} (or
 * {@link eu.solven.adhoc.encoding.string.FsstFreezingWithContext}) can normalise the values. The frozen column then
 * exposes raw {@link Utf8ByteSlice} instances to downstream consumers, where Strings are expected.
 *
 * @author Benoit Lacelle
 */
public class TestFreezerChain_Utf8 {

	@Test
	public void testLowCardinalityUtf8_isNormalisedToString() {
		// Many rows, very low cardinality (2 distinct instances out of 64 rows): triggers DistinctFreezer.
		// We reuse the same Utf8ByteSlice instances so reference-based deduplication detects only 2 distinct values
		// (Utf8ByteSlice does not override equals/hashCode).
		Utf8ByteSlice foo = Utf8ByteSlice.fromString("foo");
		Utf8ByteSlice bar = Utf8ByteSlice.fromString("bar");

		ObjectArrayColumn column = ObjectArrayColumn.builder().build();
		for (int i = 0; i < 64; i++) {
			column.append(i % 2 == 0 ? foo : bar);
		}

		SynchronousFreezingStrategy strategy = SynchronousFreezingStrategy.builder().build();
		IReadableColumn frozen = strategy.freeze(column);

		// Each readValue must return a String, never a Utf8ByteSlice.
		for (int i = 0; i < 64; i++) {
			Object value = frozen.readValue(i);
			Assertions.assertThat(value)
					.as("row %s should be a String, not a Utf8ByteSlice", i)
					.isInstanceOf(String.class)
					.isEqualTo(i % 2 == 0 ? "foo" : "bar");
		}
	}
}
