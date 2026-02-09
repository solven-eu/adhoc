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

/**
 * Encode given FSST.
 * 
 * @author Benoit Lacelle
 */
@SuppressWarnings({ "PMD", "checkstyle:all" })
class FsstEncoder {
	public static Fsst INSTANCE;

	public static boolean useNative(boolean value) {
		if (value) {
			if (!FsstJni.isLoaded()) {
				INSTANCE = new FsstJni();
				return true;
			} else {
				return false;
			}
		} else {
			INSTANCE = new FsstJava();
			return true;
		}
	}

	private static Fsst getInstance() {
		if (INSTANCE == null) {
			useNative(false);
		}
		return INSTANCE;
	}

	private FsstEncoder() {
	}

	public static SymbolTable encode(byte[] data) {
		return getInstance().encode(data);
	}

	public static byte[] decode(byte[] symbols, int[] symbolLengths, byte[] compressedData, int decompressedLength) {
		return getInstance().decode(symbols, symbolLengths, compressedData, decompressedLength);
	}

	/**
	 * @deprecated use {@link #decode(byte[], int[], byte[], int)} instead with an explicit length
	 */
	@Deprecated
	public static byte[] decode(byte[] symbols, int[] symbolLengths, byte[] compressedData) {
		return getInstance().decode(symbols, symbolLengths, compressedData);
	}
}
