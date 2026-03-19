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
package eu.solven.adhoc.encoding.fsst;

import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.bytes.IByteSlice;
import eu.solven.adhoc.encoding.bytes.Utf8ByteSlice;

public class TestSymbolTableEncoder {
	@Test
	public void testEncodeLargeSlice() {
		IFsstEncoding trained = FsstTrainer.builder().build().train("hello");

		String input = "hellohellohello";
		IByteSlice wholeInput = Utf8ByteSlice.fromString(input);
		// Crop first and last char
		IByteSlice partialInput = wholeInput.sub(1, wholeInput.length() - 2);
		Assertions.assertThat(partialInput.asString(StandardCharsets.UTF_8)).isEqualTo("ellohellohell");

		IByteSlice encoded = trained.encodeAll(partialInput);
		IByteSlice decoded = trained.decodeAll(encoded);

		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo("ellohellohell");
	}
}
