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
package eu.solven.adhoc.fsst.v3;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;
import lombok.RequiredArgsConstructor;

/**
 * Represents a stable table of symbols, enabling encoding and decoding.
 * 
 * @author Benoit Lacelle
 */
@ThreadSafe
@RequiredArgsConstructor
@SuppressWarnings("checkstyle:MagicNumber")
public class SymbolTable implements IFsstConstants, IFsstDecoder {
	// Used for encoding
	final SymbolTableTraining symbols;

	final SymbolTableDecoding decoding;

	// Encode compresses input, reusing buf if provided.
	// Returns compressed data (may be a different slice than buf).
	public ByteSlice encode(byte[] buf, byte[] input) {
		if (buf == null || buf.length < 2 * input.length + fsstOutputPadding) {
			buf = new byte[2 * input.length + fsstOutputPadding];
		}
		int outPos = 0;
		int pos = 0;
		int inputLen = input.length;

		int tailLen = inputLen % 8;

		// Process with safe unaligned loads while >=8 bytes remain
		while (pos + 8 <= inputLen) {
			int chunkEnd = Math.min(pos + fsstChunkSize, inputLen - tailLen);
			outPos = encodeChunk(buf, outPos, input, pos, chunkEnd);
			pos = chunkEnd;
		}

		// Handle tail with padded buffer
		if (pos < inputLen) {
			// makes a new buffer with padded 0
			// it enables fsstUnalignedLoad assuming the input always have expected size
			byte[] encBuf = Arrays.copyOfRange(input, pos, pos + 8);
			outPos = encodeChunk(buf, outPos, encBuf, 0, tailLen);
		}
		return new ByteSlice(buf, 0, outPos);
	}

	public ByteSlice encodeAll(byte[] input) {
		return encode(null, input);
	}

	/**
	 * 
	 * @param dst
	 *            the encoded output
	 * @param dstPos
	 *            the initial position at which to write encoded bytes
	 * @param input
	 *            the decoded input
	 * @param pos
	 *            the start position for eading input
	 * @param end
	 *            may be shorter than input (e.g. if we restrict to a chunk, or if we padded with 0)
	 * @return
	 */
	// encodeChunk compresses buf[0:end] to dst starting at dstPos.
	// buf must have >=8 bytes padding after end for safe unaligned loads.
	@SuppressWarnings("PMD.AssignmentInOperand")
	private int encodeChunk(byte[] dst, int dstPos, byte[] input, int pos, int end) {
		int suffixLimit = decoding.suffixLim;

		while (pos < end) {
			long word = SymbolUtil.fsstUnalignedLoad(input, pos);

			// Try 2-byte fast path (unique prefix)
			int code = symbols.shortCodes[(int) (word & fsstMask16)];
			if ((code & 0xFF) < suffixLimit && pos + 2 <= end) {
				dst[dstPos++] = (byte) code;

				pos += 2;
				continue;
			}

			// Try 3-8 byte hash table match
			int idx = (int) (SymbolUtil.fsstHash(word & fsstMask24) & (fsstHashTabSize - 1));
			Symbol entry = symbols.hashTab[idx];

			// Relates with SymbolTableTraining.findLongestSymbol
			if (entry.icl < fsstICLFree) {
				long mask = ~0L >>> entry.ignoredBits();
				int symLen = entry.length();
				// hash matched: let's check for equality
				if ((entry.val & mask) == (word & mask) && pos + symLen <= end) {
					dst[dstPos++] = (byte) entry.code();
					pos += symLen;
					continue;
				}
			}

			// Fall back to 2-byte (if valid)
			int advance = code >> fsstLenBits;
			if (pos + advance > end) {
				// or 1-byte/escape (if input too short)
				code = symbols.byteCodes[input[pos] & 0xFF];
				advance = 1;
			}

			dst[dstPos++] = (byte) (code & fsstMask8);
			if ((code & fsstCodeBase) != 0) {
				dst[dstPos++] = input[pos];
			}
			pos += advance;
		}
		return dstPos;
	}

	public ByteSlice encodeAll(String string) {
		return encodeAll(string.getBytes(StandardCharsets.UTF_8));
	}

	@Override
	public ByteSlice decode(byte[] buf, ByteSlice src) {
		return decoding.decode(buf, src);
	}

	@Override
	public ByteSlice decode(byte[] buf, byte[] src) {
		return decoding.decode(buf, src);
	}

	@Override
	public ByteSlice decode(byte[] buf, byte[] src, int srcStart, int srcEnd) {
		return decoding.decode(buf, src, srcStart, srcEnd);
	}

	@Override
	public ByteSlice decodeAll(byte[] src) {
		return decoding.decode(null, src);
	}

	@Override
	public ByteSlice decodeAll(ByteSlice src) {
		return decoding.decode(null, src);
	}
}