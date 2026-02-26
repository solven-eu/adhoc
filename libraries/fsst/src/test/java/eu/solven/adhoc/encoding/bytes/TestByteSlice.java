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
package eu.solven.adhoc.encoding.bytes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class TestByteSlice {
	@Test
	public void testEquals() {
		EqualsVerifier.forClass(ByteSlice.class);
	}

	@Test
	public void testLengthOffset() {
		String original = "hello";
		byte[] originalBytesStrict = original.getBytes(StandardCharsets.UTF_8);
		int offset = 2;
		byte[] offsetBytes = new byte[offset + originalBytesStrict.length + offset];

		// Make sure all inputs are non-zero, to check they are actually discarded
		Arrays.fill(offsetBytes, (byte) 7);

		System.arraycopy(originalBytesStrict, 0, offsetBytes, offset, originalBytesStrict.length);

		IByteSlice byteSlice = new ByteSlice(offsetBytes, offset, originalBytesStrict.length);

		Assertions.assertThat(byteSlice.asString(StandardCharsets.UTF_8)).isEqualTo(original);
		Assertions.assertThat(byteSlice).hasToString("[104, 101, 108, 108, 111]");

		Assertions.assertThat(byteSlice.hashCode()).isEqualTo(Arrays.hashCode(originalBytesStrict));
		Assertions.assertThat(byteSlice)
				.isEqualTo(ByteSlice.builder()
						.array(offsetBytes)
						.offset(offset)
						.length(originalBytesStrict.length)
						.build());

		Assertions.assertThat(byteSlice.array()).isSameAs(offsetBytes);
		Assertions.assertThat(byteSlice.cropped()).containsExactly(originalBytesStrict);
	}
}
