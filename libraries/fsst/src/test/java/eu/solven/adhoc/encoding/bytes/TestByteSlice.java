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
						.buffer(offsetBytes)
						.offset(offset)
						.length(originalBytesStrict.length)
						.build());

		Assertions.assertThat(byteSlice.buffer()).isSameAs(offsetBytes);
		Assertions.assertThat(byteSlice.crop()).containsExactly(originalBytesStrict);
	}

	@Test
	public void testSub() {
		IByteSlice byteSlice = IByteSlice.wrap("12345".getBytes(StandardCharsets.UTF_8));
		IByteSlice sub = byteSlice.sub(1, 3);

		// sub from byteSlice which is a ByteSliceNoOffsetNoLength
		Assertions.assertThat(sub).isInstanceOfSatisfying(ByteSlice.class, s -> {
			Assertions.assertThat(s.asString(StandardCharsets.UTF_8)).isEqualTo("234");
		});

		Assertions.assertThat(sub.sub(1, 1)).isInstanceOfSatisfying(ByteSlice.class, s -> {
			Assertions.assertThat(s.asString(StandardCharsets.UTF_8)).isEqualTo("3");
		});
	}

	@Test
	public void testBuilder_noOffsetNoLength() {
		IByteSlice byteSlice = ByteSlice.builder().buffer(new byte[] { '0', '1', '2' }).build();

		Assertions.assertThat(byteSlice).isInstanceOfSatisfying(ByteSliceNoOffsetNoLength.class, s -> {
			Assertions.assertThat(s.asString(StandardCharsets.UTF_8)).isEqualTo("012");
		});
	}

	@Test
	public void testCropped() {
		IByteSlice byteSlice = IByteSlice.wrap("12345".getBytes(StandardCharsets.UTF_8));
		Assertions.assertThat(byteSlice.crop()).isSameAs(byteSlice.buffer());

		IByteSlice sub = byteSlice.sub(1, 3);
		Assertions.assertThat(sub.crop()).isNotSameAs(byteSlice.buffer());
	}

	@Test
	public void testCropped_forcedByteSliceNoOffsetNoLength() {
		IByteSlice byteSlice = new ByteSlice("12345".getBytes(StandardCharsets.UTF_8), 0, 5);
		Assertions.assertThat(byteSlice.crop()).isSameAs(byteSlice.buffer());
	}

	@Test
	public void testSub_tooLargeLength_throws() {
		IByteSlice byteSlice = new ByteSlice("abc".getBytes(StandardCharsets.UTF_8), 0, 3);

		Assertions.assertThatThrownBy(() -> byteSlice.sub(0, 4))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("4");
	}

	@Test
	public void testEquals_sameInstance() {
		ByteSlice s = new ByteSlice("hi".getBytes(StandardCharsets.UTF_8), 0, 2);

		Assertions.assertThat(s.equals(s)).isTrue();
	}

	@Test
	public void testEquals_null() {
		ByteSlice s = new ByteSlice("hi".getBytes(StandardCharsets.UTF_8), 0, 2);

		Assertions.assertThat(s.equals(null)).isFalse();
	}

	@Test
	public void testEquals_nonByteSliceType() {
		ByteSlice s = new ByteSlice("hi".getBytes(StandardCharsets.UTF_8), 0, 2);

		Assertions.assertThat("hi".equals(s)).isFalse();
	}

	@Test
	public void testEquals_differentLength() {
		byte[] buf = "hello".getBytes(StandardCharsets.UTF_8);
		ByteSlice s1 = new ByteSlice(buf, 0, 3);
		ByteSlice s2 = new ByteSlice(buf, 0, 4);

		Assertions.assertThat(s1.equals(s2)).isFalse();
	}

	@Test
	public void testEquals_differentBytes() {
		ByteSlice s1 = new ByteSlice("abc".getBytes(StandardCharsets.UTF_8), 0, 3);
		ByteSlice s2 = new ByteSlice("abd".getBytes(StandardCharsets.UTF_8), 0, 3);

		Assertions.assertThat(s1.equals(s2)).isFalse();
	}

	@Test
	public void testIsFastCrop_trueWhenExactMatch() {
		byte[] buf = "xyz".getBytes(StandardCharsets.UTF_8);
		ByteSlice s = new ByteSlice(buf, 0, buf.length);

		Assertions.assertThat(s.isFastCrop()).isTrue();
	}

	@Test
	public void testIsFastCrop_falseWithOffset() {
		byte[] buf = "xyz".getBytes(StandardCharsets.UTF_8);
		ByteSlice s = new ByteSlice(buf, 1, buf.length - 1);

		Assertions.assertThat(s.isFastCrop()).isFalse();
	}

	@Test
	public void testIsFastCrop_falseWhenLengthShorterThanBuffer() {
		byte[] buf = "xyz".getBytes(StandardCharsets.UTF_8);
		ByteSlice s = new ByteSlice(buf, 0, buf.length - 1);

		Assertions.assertThat(s.isFastCrop()).isFalse();
	}

	@Test
	public void testStaticHashCode_emptyArray() {
		int h = ByteSlice.hashCode(new byte[0], 0, 0);

		Assertions.assertThat(h).isEqualTo(1);
	}

	@Test
	public void testStaticHashCode_subRange() {
		byte[] buf = "abcde".getBytes(StandardCharsets.UTF_8);
		int h = ByteSlice.hashCode(buf, 1, 3);
		int expected = ByteSlice.hashCode("bcd".getBytes(StandardCharsets.UTF_8), 0, 3);

		Assertions.assertThat(h).isEqualTo(expected);
	}

	@Test
	public void testBuilder_withOffset_noLength_usesBufferLength() {
		byte[] buf = "abcde".getBytes(StandardCharsets.UTF_8);

		IByteSlice byteSlice = ByteSlice.builder().buffer(buf).offset(2).build();

		Assertions.assertThat(byteSlice).isInstanceOf(ByteSlice.class);
		Assertions.assertThat(byteSlice.offset()).isEqualTo(2);
		Assertions.assertThat(byteSlice.length()).isEqualTo(buf.length);
	}

	@Test
	public void testBuilder_offsetZero_withLength_createsNoOffsetVariant() {
		byte[] buf = "abcde".getBytes(StandardCharsets.UTF_8);

		IByteSlice byteSlice = ByteSlice.builder().buffer(buf).offset(0).length(3).build();

		Assertions.assertThat(byteSlice).isInstanceOf(ByteSliceNoOffset.class);
		Assertions.assertThat(byteSlice.length()).isEqualTo(3);
	}

}
