/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
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

import com.google.common.base.Strings;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.UtilityClass;

/**
 * Core constants for FSST compression algorithm
 *
 * Represents a Symbol in FSST. A Symbol is a contiguous chunk of bytes which can be represented by a shorter
 * representation (e.g. a single byte).
 *
 * @author Benoit Lacelle
 */
// https://github.com/axiomhq/fsst/blob/main/symbol.go
@SuppressWarnings("checkstyle:MagicNumber")
@UtilityClass
public final class SymbolUtil implements IFsstConstants {

	public static long fsstUnalignedLoad(byte[] in, int offset) {
		// ByteBuffer buf = ByteBuffer.wrap(b, offset, 8);
		// buf.order(ByteOrder.LITTLE_ENDIAN);
		// return buf.getLong();
		int length = Math.min(in.length - offset, 8);

		long value = switch (length) {
		case 1 -> (in[offset] & 0xFFL);
		case 2, 3, 4, 5, 6, 7 -> {
			long v = 0;
			for (int i = 0; i < length; i++) {
				// make sure we cast to long BEFORE bit-shifting
				v |= (in[i + offset] & 0xFFL) << (8 * i);
			}
			yield v;
		}
		// BEWARE JMH suggests unrolling the loop has no benefit
		default -> (in[offset] & 0xFFL) | (in[offset + 1] & 0xFFL) << 8
				| (in[offset + 2] & 0xFFL) << 16
				| (in[offset + 3] & 0xFFL) << 24
				| (in[offset + 4] & 0xFFL) << 32
				| (in[offset + 5] & 0xFFL) << 40
				| (in[offset + 6] & 0xFFL) << 48
				| (in[offset + 7] & 0xFFL) << 56;
		};

		return value;
	}

	public static long fsstHash(long w) {
		long x = w * fsstHashPrime;
		return x ^ (x >>> fsstShift);
	}

	// packCodeLength combines a code and length into a packed uint16 used by byteCodes/shortCodes.
	// Format: (length << 12) | code, where length is 1-8 and code is 0-511.
	public static int packCodeLength(int code, int length) {
		return (code & 0xFFFF) | ((length << fsstLenBits) & 0xFFFF);
	}

	/**
	 * symbol is the internal representation of a compression symbol (1-8 bytes). // It packs the symbol value and
	 * metadata into two uint64 fields:
	 */
	// val: the actual symbol bytes in little-endian (up to 8 bytes)
	// icl: packed metadata with bit layout:
	// bits 28-31: length (1-8)
	// bits 16-27: code (0-511)
	// bits 0-15: ignoredBits = (8-length)*8, used for hash table masking
	@AllArgsConstructor(access = AccessLevel.PROTECTED)
	public static final class Symbol {
		long val; // Symbol bytes in little-endian
		long icl; // Packed: [length:4][code:12][ignoredBits:16]

		public static Symbol newSymbolFromByte(byte b, int code) {
			long val = b & 0xFF;
			return new Symbol(val, evalICL(code, 1));
		}

		public static Symbol newSymbolFromBytes(byte[] in) {
			return fromBytes(in, 0);
		}

		public static Symbol fromBytes(byte[] in, int offset) {
			int length = Math.min(in.length - offset, 8);
			// assert length > 0;
			long value = fsstUnalignedLoad(in, offset);
			return new Symbol(value, evalICL(fsstCodeMax, length));
		}

		public static long evalICL(int code, int length) {
			assert code >= 0;
			// If code==fsstCodeMask, it comes from a merge. Only case?
			assert code <= fsstCodeMax;
			assert length >= 0;
			assert length <= 8;

			// icl format: [length:4][code:12][ignoredBits:16]
			// length=1, ignoredBits=(8-1)*8=56 (ignore top 7 bytes when matching)
			return ((long) length << 28) | ((long) code << 16) | ((8 - length) * 8);
		}

		public int length() {
			return (int) (icl >>> 28);
		}

		public int code() {
			return (int) ((icl >>> 16) & fsstCodeMask);
		}

		public int ignoredBits() {
			return (int) (icl & fsstMask16);
		}

		public int first() {
			return (int) (val & fsstMask8);
		}

		public int first2() {
			return (int) (val & fsstMask16);
		}

		public int hash() {
			long hash = fsstHash(val & fsstMask24);
			return (int) (hash & (fsstHashTabSize - 1));
		}

		public Symbol withCode(int code) {
			int length = length();
			return new Symbol(val, evalICL(code, length));
		}

		@Override
		public String toString() {
			// each byte is represented by 2 chars
			String hexString = Long.toHexString(Long.reverseBytes(val));
			int length = 2 * length();
			String valueAsString = Strings.padStart(hexString, length, '0').substring(0, length);

			byte[] bytes = new byte[length()];
			for (int i = 0; i < length(); i++) {
				bytes[i] = (byte) ((val >> 8 * i) & 0xFF);
			}

			return "length=%s code=%s value=%s UTF-8=%s"
					.formatted(length(), code(), valueAsString, new String(bytes, StandardCharsets.UTF_8));
		}
	}

	public static Symbol fsstConcat(Symbol a, Symbol b) {
		int lengthA = a.length();
		int lengthB = b.length();
		int combinedLength = Math.min(lengthA + lengthB, 8);
		// Append b.val after a.val: b may be cut on its higher bits (to the left of val)
		long combinedValue = (b.val << (8 * lengthA)) | a.val;
		// code=fsstCodeMask will be replaced later?
		return new Symbol(combinedValue, Symbol.evalICL(fsstCodeMask, combinedLength));
	}

	@Deprecated
	public static Symbol fsstConcatEnd(Symbol a, Symbol b) {
		int lengthA = a.length();
		int lengthB = b.length();

		if (lengthA + lengthB <= 8) {
			throw new IllegalArgumentException("For perf reason, this case should never happen");
		}

		int combinedLength = 8;
		int shiftA = 8 * (lengthA - (8 - lengthB));

		// Append b.val after a.val: a may be cut on its lower bits (to the right of val)
		long combinedValue = (b.val << (8 * (lengthA) - shiftA)) | (a.val >>> shiftA);
		// code=fsstCodeMask will be replaced later?
		return new Symbol(combinedValue, Symbol.evalICL(fsstCodeMask, combinedLength));
	}

}