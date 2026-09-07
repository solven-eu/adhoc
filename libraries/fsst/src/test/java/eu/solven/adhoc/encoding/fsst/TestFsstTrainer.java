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
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.bytes.IByteSlice;

/**
 * Tests for {@link FsstTrainer}, covering branches not exercised by higher-level encode/decode tests.
 */
public class TestFsstTrainer {

	FsstTrainer trainer = FsstTrainer.builder().build();

	// ── isEscapeCode ──────────────────────────────────────────────────────────

	@Test
	public void testIsEscapeCode_literalByte_returnsTrue() {
		// Codes 0–255 are escape codes (literal bytes)
		Assertions.assertThat(trainer.isEscapeCode(0)).isTrue();
		Assertions.assertThat(trainer.isEscapeCode(128)).isTrue();
		Assertions.assertThat(trainer.isEscapeCode(255)).isTrue();
	}

	@Test
	public void testIsEscapeCode_symbolCode_returnsFalse() {
		// Codes >= fsstCodeBase (256) are real symbol codes
		Assertions.assertThat(trainer.isEscapeCode(IFsstConstants.fsstCodeBase)).isFalse();
		Assertions.assertThat(trainer.isEscapeCode(400)).isFalse();
	}

	// ── encodedLength ─────────────────────────────────────────────────────────

	@Test
	public void testEncodedLength_escapedCode_takes2Bytes() {
		// An escaped byte is written as: escape-marker byte + literal byte = 2 bytes
		Assertions.assertThat(trainer.encodedLength(0)).isEqualTo(2);
		Assertions.assertThat(trainer.encodedLength(100)).isEqualTo(2);
	}

	@Test
	public void testEncodedLength_symbolCode_takes1Byte() {
		// A symbol code is written as a single byte
		Assertions.assertThat(trainer.encodedLength(IFsstConstants.fsstCodeBase)).isEqualTo(1);
		Assertions.assertThat(trainer.encodedLength(400)).isEqualTo(1);
	}

	// ── train overloads ───────────────────────────────────────────────────────

	@Test
	public void testTrain_singleByteArray() {
		byte[] corpus = "hello world hello world".getBytes(StandardCharsets.UTF_8);
		IFsstEncoding encoding = trainer.train(corpus);

		IByteSlice decoded = encoding.decodeAll(encoding.encodeAll("hello world"));
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo("hello world");
	}

	@Test
	public void testTrain_byteSliceList() {
		List<IByteSlice> inputs = List.of(IByteSlice.wrap("hello world hello world".getBytes(StandardCharsets.UTF_8)));
		IFsstEncoding encoding = trainer.train(inputs);

		IByteSlice decoded = encoding.decodeAll(encoding.encodeAll("hello world"));
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo("hello world");
	}

	@Test
	public void testTrain_stringVarargs() {
		IFsstEncoding encoding = trainer.train("hello world hello world", "foo bar baz foo bar baz");

		IByteSlice decoded = encoding.decodeAll(encoding.encodeAll("hello world foo bar"));
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo("hello world foo bar");
	}

	// ── makeSample: small input path (no sampling) ────────────────────────────

	@Test
	public void testTrainOverStrings_smallInput_noSampling() {
		// Input well below sampleTarget (16 KB): no sampling occurs (isSampled=false path)
		IFsstEncoding encoding = trainer.trainOverStrings(List.of("hello"));

		IByteSlice decoded = encoding.decodeAll(encoding.encodeAll("hello"));
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo("hello");
	}

	// ── makeSample: large input path (sampling) ────────────────────────────────

	@Test
	public void testTrainOverStrings_largeInput_triggersSampling() {
		// Build input > 16 KB (default sampleTarget) to exercise the sampling branch
		StringBuilder sb = new StringBuilder();
		while (sb.length() < 20_000) {
			sb.append("hello world foo bar baz ");
		}
		IFsstEncoding encoding = trainer.trainOverStrings(List.of(sb.toString()));

		IByteSlice decoded = encoding.decodeAll(encoding.encodeAll("hello world"));
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo("hello world");
	}

	// ── compressCount / buildCandidates via train ──────────────────────────────

	@Test
	public void testTrain_multipleStrings_convergesWithoutException() {
		// Exercises multiple compressCount passes (frac 8, 38, 68, 98, 128)
		IFsstEncoding encoding = trainer.trainOverStrings(List.of("abcabcabc",
				"xyzxyzxyz",
				"hello hello hello hello hello hello hello",
				"the the the the the the the the the"));

		// Verify encode/decode round-trips correctly for a known string
		String test = "abcxyz";
		IByteSlice decoded = encoding.decodeAll(encoding.encodeAll(test));
		Assertions.assertThat(decoded.asString(StandardCharsets.UTF_8)).isEqualTo(test);
	}
}
