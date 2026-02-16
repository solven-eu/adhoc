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
 * Like {@link ByteSlice} with offset==0 and length matching the input.
 * 
 * @author Benoit Lacelle
 */
@AllArgsConstructor
final class ByteSliceNoOffsetNoLength implements IByteSlice {
	final byte[] array;

	@Override
	public boolean isFastAsArray() {
		return true;
	}

	@Override
	public byte[] refHolderArray() {
		return array;
	}

	@Override
	public int length() {
		return array.length;
	}

	@Override
	public int offset() {
		return 0;
	}

	/**
	 * 
	 * @return a non-defensive copy of this as a `byte[]`.
	 */
	@SuppressWarnings("PMD.MethodReturnsInternalArray")
	@Override
	public byte[] asByteArray() {
		return array;
	}

	// Duplicated from jdk.internal.util.ArraysSupport.hashCode(int, byte[], int, int)
	@Override
	public int hashCode() {
		return Arrays.hashCode(array);
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
		ByteSliceNoOffsetNoLength other = (ByteSliceNoOffsetNoLength) obj;
		return Arrays.equals(array, other.array);
	}

	@Override
	public byte read(int position) {
		return array[position];
	}

}