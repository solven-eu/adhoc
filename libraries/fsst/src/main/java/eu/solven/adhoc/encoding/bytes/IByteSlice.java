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

import java.nio.charset.Charset;

/**
 * A byte wrapper, enabling some sub-byte[] without creating a new array.
 * 
 * @author Benoit Lacelle
 */
public interface IByteSlice {

	/**
	 * Useful is critical section when one prefer to operate on a `byte[]`.
	 * 
	 * @return true if {@link #cropped()} is expected to be very fast (e.g. returning a reference).
	 */
	boolean isFastAsArray();

	/**
	 * May or may not be the original array
	 * 
	 * @return a non-defensive copy of this as a `byte[]`.
	 */
	byte[] cropped();

	byte read(int position);

	/**
	 * The inner array
	 * 
	 * @return a non-defensive reference to the internal array
	 */
	byte[] array();

	int length();

	int offset();

	/**
	 * 
	 * @param charset
	 * @return a {@link String} build over current {@link IByteSlice}.
	 */
	default String asString(Charset charset) {
		return new String(array(), offset(), length(), charset);
	}

	static IByteSlice wrap(byte[] input) {
		return new ByteSliceNoOffsetNoLength(input);
	}

	static IByteSlice wrap(byte[] array, int length) {
		return new ByteSliceNoOffset(array, length);
	}

	static IByteSlice wrap(byte[] array, int offset, int length) {
		return new ByteSlice(array, offset, length);
	}

	IByteSlice sub(int off, int length);
}