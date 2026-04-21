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

public class TestByteSliceNoOffset {

	@Test
	public void testLengthOffset() {
		String original = "hello";
		byte[] originalBytesStrict = original.getBytes(StandardCharsets.UTF_8);
		int suffix = 2;
		byte[] offsetBytes = new byte[originalBytesStrict.length + suffix];

		// Make sure all inputs are non-zero, to check they are actually discarded
		Arrays.fill(offsetBytes, (byte) 7);

		// Copy the content with a suffix
		System.arraycopy(originalBytesStrict, 0, offsetBytes, 0, originalBytesStrict.length);

		IByteSlice byteSlice = new ByteSliceNoOffset(offsetBytes, originalBytesStrict.length);

		Assertions.assertThat(byteSlice.asString(StandardCharsets.UTF_8)).isEqualTo(original);
		Assertions.assertThat(byteSlice).hasToString("[104, 101, 108, 108, 111]");

		Assertions.assertThat(byteSlice.hashCode()).isEqualTo(Arrays.hashCode(originalBytesStrict));
		Assertions.assertThat(byteSlice)
				.isEqualTo(ByteSlice.builder().buffer(offsetBytes).length(originalBytesStrict.length).build());

		Assertions.assertThat(byteSlice.buffer()).isSameAs(offsetBytes);
		Assertions.assertThat(byteSlice.crop()).containsExactly(originalBytesStrict);
	}

	@Test
	public void testSub() {
		IByteSlice byteSlice = ByteSlice.builder().buffer("hello".getBytes(StandardCharsets.UTF_8)).length(3).build();

		Assertions.assertThat(byteSlice).isInstanceOf(ByteSliceNoOffset.class);

		Assertions.assertThat(byteSlice.sub(1, 1).asString(StandardCharsets.UTF_8)).isEqualTo("e");
	}

	@Test
	public void testCropped_forced() {
		IByteSlice suboptimal = new ByteSliceNoOffset("hello".getBytes(StandardCharsets.UTF_8), 5);
		Assertions.assertThat(suboptimal).isInstanceOf(ByteSliceNoOffset.class);

		Assertions.assertThat(suboptimal.crop()).isSameAs(suboptimal.buffer());
	}

	@Test
	public void testSub_withOffset_returnsOffsetSlice() {
		byte[] buf = "hello".getBytes(StandardCharsets.UTF_8);
		IByteSlice slice = new ByteSliceNoOffset(buf, buf.length);

		// sub with non-zero offset should produce a ByteSlice (with offset), not ByteSliceNoOffset
		IByteSlice sub = slice.sub(1, 3);
		Assertions.assertThat(sub.asString(StandardCharsets.UTF_8)).isEqualTo("ell");
	}

	@Test
	public void testSub_tooLong_throws() {
		byte[] buf = "hi".getBytes(StandardCharsets.UTF_8);
		IByteSlice slice = new ByteSliceNoOffset(buf, buf.length);

		Assertions.assertThatThrownBy(() -> slice.sub(0, buf.length + 1)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testEquals_null_returnsFalse() {
		IByteSlice slice = new ByteSliceNoOffset("a".getBytes(StandardCharsets.UTF_8), 1);

		Assertions.assertThat(slice.equals(null)).isFalse();
	}

	@Test
	public void testEquals_sameInstance_returnsTrue() {
		IByteSlice slice = new ByteSliceNoOffset("a".getBytes(StandardCharsets.UTF_8), 1);

		Assertions.assertThat(slice.equals(slice)).isTrue();
	}

	@Test
	public void testEquals_nonByteSlice_returnsFalse() {
		IByteSlice slice = new ByteSliceNoOffset("a".getBytes(StandardCharsets.UTF_8), 1);

		Assertions.assertThat("a".equals(slice)).isFalse();
	}

	@Test
	public void testIsFastCrop_exactLength_returnsTrue() {
		byte[] buf = "abc".getBytes(StandardCharsets.UTF_8);
		IByteSlice slice = new ByteSliceNoOffset(buf, buf.length);

		Assertions.assertThat(slice.isFastCrop()).isTrue();
	}

	@Test
	public void testIsFastCrop_shorterLength_returnsFalse() {
		byte[] buf = "abc".getBytes(StandardCharsets.UTF_8);
		IByteSlice slice = new ByteSliceNoOffset(buf, 2);

		Assertions.assertThat(slice.isFastCrop()).isFalse();
	}

	@Test
	public void testRead_returnsCorrectByte() {
		byte[] buf = "abc".getBytes(StandardCharsets.UTF_8);
		IByteSlice slice = new ByteSliceNoOffset(buf, buf.length);

		Assertions.assertThat(slice.read(0)).isEqualTo((byte) 'a');
		Assertions.assertThat(slice.read(1)).isEqualTo((byte) 'b');
	}
}
