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

	@Test
	public void testEncodeEmpty_roundTrip() {
		// Empty input: no main loop, no tail
		IFsstEncoding trained = FsstTrainer.builder().build().train("hello");
		IByteSlice empty = IByteSlice.wrap(new byte[0]);
		IByteSlice encoded = trained.encodeAll(empty);
		IByteSlice decoded = trained.decodeAll(encoded);
		Assertions.assertThat(decoded.length()).isZero();
	}

	@Test
	public void testEncode_fastCropArrayPath_roundTrip() {
		// Utf8ByteSlice.fromString wraps ByteSliceNoOffsetNoLength (isFastCrop=true)
		// so the encoder routes through encodeArray(), not encodeSlice()
		IFsstEncoding trained = FsstTrainer.builder().build().train("hello");
		String input = "hellohellohello";
		IByteSlice arrayInput = Utf8ByteSlice.fromString(input);
		Assertions.assertThat(arrayInput.isFastCrop()).isTrue();

		IByteSlice encoded = trained.encodeAll(arrayInput);
		IByteSlice decoded = trained.decodeAll(encoded);
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo(input);
	}

	@Test
	public void testEncode_shortInput_lessThan8Bytes() {
		// Input shorter than 8 bytes: no main chunk loop, only tail processing
		IFsstEncoding trained = FsstTrainer.builder().build().train("ab");
		IByteSlice input = Utf8ByteSlice.fromString("abc");
		Assertions.assertThat(input.length()).isLessThan(8);

		IByteSlice encoded = trained.encodeAll(input);
		IByteSlice decoded = trained.decodeAll(encoded);
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo("abc");
	}

	@Test
	public void testEncode_exactly8Bytes() {
		// Input exactly 8 bytes: main loop runs once, no tail
		IFsstEncoding trained = FsstTrainer.builder().build().train("abcdefgh");
		String input = "abcdefgh";
		IByteSlice inputSlice = Utf8ByteSlice.fromString(input);
		Assertions.assertThat(inputSlice.length()).isEqualTo(8);

		IByteSlice encoded = trained.encodeAll(inputSlice);
		IByteSlice decoded = trained.decodeAll(encoded);
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo(input);
	}

	@Test
	public void testEncode_largerThanChunkSize() {
		// fsstChunkSize = 511; input > 511 bytes triggers multi-chunk encoding path
		String base = "hello world ";
		StringBuilder sb = new StringBuilder();
		while (sb.length() < 600) {
			sb.append(base);
		}
		String input = sb.substring(0, 600);
		IFsstEncoding trained = FsstTrainer.builder().build().train(base);
		IByteSlice inputSlice = Utf8ByteSlice.fromString(input);
		Assertions.assertThat(inputSlice.length()).isGreaterThan(511);

		IByteSlice encoded = trained.encodeAll(inputSlice);
		IByteSlice decoded = trained.decodeAll(encoded);
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo(input);
	}

	@Test
	public void testEncode_preallocatedBuffer_sufficientSize() {
		// encode(buf, input) with a large enough buf → buf is reused (not reallocated)
		IFsstEncoding trained = FsstTrainer.builder().build().train("hello");
		String input = "hellohellohello";
		IByteSlice inputSlice = Utf8ByteSlice.fromString(input);
		byte[] buf = new byte[1024]; // well above 2*15+7=37 bytes

		IByteSlice encoded = trained.encode(buf, inputSlice);
		Assertions.assertThat(encoded.buffer()).isSameAs(buf);

		IByteSlice decoded = trained.decodeAll(encoded);
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo(input);
	}

	@Test
	public void testDecode_srcEndEqualsArrayLength_throws() {
		// SymbolTableDecoder.decode(buf, src, 0, src.length) with srcEnd == src.length
		// triggers the "srcEnd >= src.length" guard → IllegalArgumentException
		IFsstEncoding trained = FsstTrainer.builder().build().train("hello");
		IFsstDecoder decoder = trained.asDecoder();
		byte[] src = new byte[] { 0x01, 0x02 };
		Assertions.assertThatThrownBy(() -> decoder.decode(null, src, 0, src.length))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testDecode_tinyBuffer_growsOnDemand() {
		// Decoding with a tiny initial buffer forces the decoder to extend it internally
		IFsstEncoding trained = FsstTrainer.builder().build().train("hello world");
		String input = "hello world hello world hello world hello world";
		IByteSlice encoded = trained.encodeAll(Utf8ByteSlice.fromString(input));

		byte[] tinyBuf = new byte[4];
		IByteSlice decoded = trained.decode(tinyBuf, encoded);
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo(input);
	}
}
