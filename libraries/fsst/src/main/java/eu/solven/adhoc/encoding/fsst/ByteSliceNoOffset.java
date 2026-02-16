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

import java.util.Arrays;

import lombok.AllArgsConstructor;

/**
 * A byte wrapper, enabling some sub-byte[] without creating a new array.
 * 
 * Like {@link ByteSlice} with offset==0.
 * 
 * @author Benoit Lacelle
 */
@AllArgsConstructor
public final class ByteSliceNoOffset implements IByteSlice {
	final byte[] array;
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	final int length;

	@Override
	public boolean isFastAsArray() {
		return false;
	}

	@Override
	public int length() {
		return length;
	}

	/**
	 * 
	 * @return a non-defensive copy of this as a `byte[]`.
	 */
	@SuppressWarnings("PMD.MethodReturnsInternalArray")
	@Override
	public byte[] asByteArray() {
		if (array.length == length) {
			return array;
		} else {
			return Arrays.copyOfRange(array, 0, length);
		}
	}

	// Duplicated from jdk.internal.util.ArraysSupport.hashCode(int, byte[], int, int)
	@Override
	@SuppressWarnings("checkstyle:MagicNumber")
	public int hashCode() {
		int result = 0;
		int fromIndex = 0;
		int end = fromIndex + length;
		for (int i = fromIndex; i < end; i++) {
			result = 31 * result + array[i];
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		ByteSliceNoOffset other = (ByteSliceNoOffset) obj;
		if (length != other.length) {
			return false;
		}

		int fromIndex = 0;
		int end = fromIndex + length;
		for (int i = fromIndex; i < end; i++) {
			if (this.array[i] != other.array[i]) {
				return false;
			}
		}

		return true;
	}

	@Override
	public byte read(int position) {
		return array[position];
	}

}