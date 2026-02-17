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
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestByteSliceNoOffsetNoLength {

	@Test
	public void testToString() {
		String original = "hello";
		byte[] originalBytes = original.getBytes(StandardCharsets.UTF_8);
		IByteSlice byteSlice = IByteSlice.wrap(originalBytes);

		Assertions.assertThat(byteSlice).isInstanceOf(ByteSliceNoOffsetNoLength.class);

		Assertions.assertThat(byteSlice.asString(StandardCharsets.UTF_8)).isEqualTo(original);

		Assertions.assertThat(byteSlice.hashCode()).isEqualTo(Arrays.hashCode(originalBytes));
		Assertions.assertThat(byteSlice).isEqualTo(IByteSlice.wrap(originalBytes));

		Assertions.assertThat(byteSlice.array()).isSameAs(originalBytes);
		Assertions.assertThat(byteSlice.cropped()).isSameAs(originalBytes);
	}

	@Test
	public void testLengthOffset() {
		String original = "hello";
		byte[] originalBytes = original.getBytes(StandardCharsets.UTF_8);

		IByteSlice byteSlice = new ByteSliceNoOffsetNoLength(originalBytes);

		Assertions.assertThat(byteSlice.asString(StandardCharsets.UTF_8)).isEqualTo(original);
		Assertions.assertThat(byteSlice).hasToString("[104, 101, 108, 108, 111]");

		Assertions.assertThat(byteSlice.hashCode()).isEqualTo(Arrays.hashCode(originalBytes));
		Assertions.assertThat(byteSlice).isEqualTo(ByteSlice.builder().array(originalBytes).build());

		Assertions.assertThat(byteSlice.array()).isSameAs(originalBytes);
		Assertions.assertThat(byteSlice.cropped()).containsExactly(originalBytes);
	}

	@Test
	public void testArrayNonEmptyLengthZero() {
		String original = "hello";
		byte[] originalBytesStrict = original.getBytes(StandardCharsets.UTF_8);

		IByteSlice byteSlice = ByteSlice.builder().array(originalBytesStrict).length(0).build();

		Assertions.assertThat(byteSlice.asString(StandardCharsets.UTF_8)).isEqualTo("");

		Assertions.assertThat(byteSlice.hashCode()).isEqualTo(Arrays.hashCode(new byte[0]));

		Assertions.assertThat(byteSlice.array()).containsExactly(originalBytesStrict);
		Assertions.assertThat(byteSlice.cropped()).containsExactly(new byte[0]);
	}
}
