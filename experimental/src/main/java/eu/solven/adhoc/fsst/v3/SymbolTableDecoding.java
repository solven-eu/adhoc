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

import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

import lombok.RequiredArgsConstructor;

/**
 * Represents a stable table of symbols, enabling encoding and decoding.
 * 
 * @author Benoit Lacelle
 */
@ThreadSafe
@RequiredArgsConstructor
@SuppressWarnings("checkstyle:MagicNumber")
public class SymbolTableDecoding implements IFsstConstants {
	// Decoder tables
	public final byte[] decLen; // code -> symbol length
	public final long[] decSymbol; // code -> symbol value

	// 2-bytes lower than this has no longer symbol with same prefix
	final int suffixLim;

	private ByteSlice decode(byte[] buf, ByteSlice src) {
		return decode(buf, src.array, src.offset, src.offset + src.length);
	}

	public ByteSlice decode(byte[] buf, byte[] src) {
		return decode(buf, src, 0, src.length);
	}

	@SuppressWarnings("PMD.AssignmentInOperand")
	public ByteSlice decode(byte[] buf, byte[] src, int srcStart, int srcEnd) {
		if (buf == null) {
			buf = new byte[src.length * 4 + 8];
		}

		if (srcEnd >= src.length) {
			throw new IllegalArgumentException();
		}

		int bufPos = 0;
		int srcPos = srcStart;
		int bufCap = buf.length;

		while (srcPos < srcEnd) {
			int code = src[srcPos++] & 0xFF;
			if (code < fsstEscapeCode) {
				int symLen = decLen[code];
				long symVal = decSymbol[code];

				if (bufPos + symLen > bufCap) {
					// extends the buffer as it it too small to accept decoded bytes
					int newCap = Math.max(bufCap * 2, bufPos + symLen);
					buf = Arrays.copyOf(buf, newCap);
					bufCap = newCap;
				}

				for (int i = 0; i < symLen; i++) {
					buf[bufPos + i] = (byte) ((symVal >> (8 * i)) & 0xFF);
				}
				bufPos += symLen;
			} else {
				if (srcPos >= srcEnd) {
					break;
				}
				if (bufPos >= bufCap) {
					buf = Arrays.copyOf(buf, Math.max(bufCap * 2, bufPos + 1));
				}
				buf[bufPos++] = src[srcPos++];
			}
		}
		return new ByteSlice(buf, 0, bufPos);
	}

	public ByteSlice decodeAll(byte[] src) {
		return decode(null, src);
	}

	public ByteSlice decodeAll(ByteSlice src) {
		return decode(null, src);
	}
}