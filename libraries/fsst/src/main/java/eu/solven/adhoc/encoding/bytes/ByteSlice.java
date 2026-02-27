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

import java.util.Arrays;

import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * A byte wrapper, enabling some sub-byte[] without creating a new array.
 * 
 * @author Benoit Lacelle
 */
@AllArgsConstructor
@Builder
@SuppressWarnings("PMD.PublicMemberInNonPublicType")
final class ByteSlice implements IByteSlice {
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	public final byte[] array;
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	public final int offset;
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	public final int length;

	@Override
	public boolean isFastAsArray() {
		return false;
	}

	@SuppressWarnings("PMD.MethodReturnsInternalArray")
	@Override
	public byte[] array() {
		return array;
	}

	@Override
	public int length() {
		return length;
	}

	@Override
	public int offset() {
		return offset;
	}

	@Override
	public String toString() {
		return Arrays.toString(cropped());
	}

	/**
	 * 
	 * @return a non-defensive copy of this as a `byte[]`.
	 */
	@SuppressWarnings("PMD.MethodReturnsInternalArray")
	@Override
	public byte[] cropped() {
		if (offset == 0 && array.length == length) {
			return array;
		} else {
			return Arrays.copyOfRange(array, offset, offset + length);
		}
	}

	// Duplicated from jdk.internal.util.ArraysSupport.hashCode(int, byte[], int, int)
	@Override
	public int hashCode() {
		return hashCode(array, offset, length);
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	static int hashCode(byte[] array, int offset, int length) {
		int result = 1;
		int fromIndex = offset;
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
		if (!(obj instanceof IByteSlice otherByteSlice)) {
			return false;
		}

		return equals(this, otherByteSlice);
	}

	public static boolean equals(IByteSlice left, IByteSlice right) {
		int length = left.length();
		if (length != right.length()) {
			return false;
		}

		for (int i = 0; i < length; i++) {
			if (left.read(i) != right.read(i)) {
				return false;
			}
		}

		return true;
	}

	@Override
	public byte read(int position) {
		return array[offset + position];
	}

	@Override
	public IByteSlice sub(int off, int length) {
		// TODO assert inputs are more restrictive than current filter
		return ByteSlice.builder().array(array).offset(this.offset + off).length(length).build();
	}

	/**
	 * Lombok @Builder
	 */
	public static class ByteSliceBuilder {
		// By default, there is no offset
		public int offset = 0;
		// By default,the length comes from the array
		public int length = -1;

		public IByteSlice build() {
			if (offset == -1) {
				if (length == -1) {
					return new ByteSliceNoOffsetNoLength(array);
				} else {
					return new ByteSliceNoOffset(array, length);
				}
			}
			if (length == -1) {
				length = array.length;
			}
			return new ByteSlice(array, offset, length);
		}
	}

}