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
package org.maplibre.mlt.converter.encodings.fsst;

import java.io.ByteArrayOutputStream;

/**
 * Abstract FSST in its usage by MapTitle.
 * 
 * @author Benoit Lacelle
 */
@SuppressWarnings({ "PMD", "checkstyle:all" })
public interface Fsst {

	SymbolTable encode(byte[] data);

	default byte[] decode(SymbolTable encoded) {
		return decode(encoded.symbols(),
				encoded.symbolLengths(),
				encoded.compressedData(),
				encoded.decompressedLength());
	}

	default byte[] decode(byte[] symbols, int[] symbolLengths, byte[] compressedData, int decompressedLength) {
		// optimized decoder that knows the output size so pre-allocates the array to avoid dynamically
		// allocating a ByteArrayOutputStream
		int idx = 0;
		byte[] output = new byte[decompressedLength];

		int[] symbolOffsets = new int[symbolLengths.length];
		for (int i = 1; i < symbolLengths.length; i++) {
			symbolOffsets[i] = symbolOffsets[i - 1] + symbolLengths[i - 1];
		}

		for (int i = 0; i < compressedData.length; i++) {
			// In Java a byte[] is signed [-128 to 127], whereas in C++ it is unsigned [0 to 255]
			// So we do a bit shifting operation to convert the values into unsigned values for easier
			// handling
			int symbolIndex = compressedData[i] & 0xFF;
			// 255 is our escape byte -> take the next symbol as it is
			if (symbolIndex == 255) {
				output[idx++] = compressedData[++i];
			} else if (symbolIndex < symbolLengths.length) {
				int len = symbolLengths[symbolIndex];
				System.arraycopy(symbols, symbolOffsets[symbolIndex], output, idx, len);
				idx += len;
			}
		}

		return output;
	}

	/**
	 * @deprecated use {@link #decode(byte[], int[], byte[], int)} instead with an explicit length
	 */
	@Deprecated
	default byte[] decode(byte[] symbols, int[] symbolLengths, byte[] compressedData) {
		ByteArrayOutputStream decodedData = new ByteArrayOutputStream();

		int[] symbolOffsets = new int[symbolLengths.length];
		for (int i = 1; i < symbolLengths.length; i++) {
			symbolOffsets[i] = symbolOffsets[i - 1] + symbolLengths[i - 1];
		}

		for (int i = 0; i < compressedData.length; i++) {
			// In Java a byte[] is signed [-128 to 127], whereas in C++ it is unsigned [0 to 255]
			// So we do a bit shifting operation to convert the values into unsigned values for easier
			// handling
			int symbolIndex = compressedData[i] & 0xFF;
			// 255 is our escape byte -> take the next symbol as it is
			if (symbolIndex == 255) {
				decodedData.write(compressedData[++i] & 0xFF);
			} else if (symbolIndex < symbolLengths.length) {
				decodedData.write(symbols, symbolOffsets[symbolIndex], symbolLengths[symbolIndex]);
			}
		}

		return decodedData.toByteArray();
	}
}
