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

/**
 * A byte wrapper, enabling some sub-byte[] without creating a new array.
 * 
 * Like {@link ByteSlice} with offset==0.
 * 
 * @author Benoit Lacelle
 */
@AllArgsConstructor
final class ByteSliceNoOffset implements IByteSlice {
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	final byte[] buffer;
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	final int length;

	@SuppressWarnings("PMD.MethodReturnsInternalArray")
	@Override
	public byte[] buffer() {
		return buffer;
	}

	@Override
	public int length() {
		return length;
	}

	@Override
	public int offset() {
		return 0;
	}

	@Override
	public String toString() {
		return Arrays.toString(crop());
	}

	@Override
	public boolean isFastCrop() {
		return buffer.length == length;
	}

	/**
	 * 
	 * @return a non-defensive copy of this as a `byte[]`.
	 */
	@SuppressWarnings("PMD.MethodReturnsInternalArray")
	@Override
	public byte[] crop() {
		if (buffer.length == length) {
			return buffer;
		} else {
			return Arrays.copyOfRange(buffer, 0, length);
		}
	}

	@Override
	public int hashCode() {
		return ByteSlice.hashCode(buffer, 0, length);
	}

	@Override
	public IByteSlice sub(int off, int length) {
		if (length > length()) {
			throw new IllegalArgumentException("%s > %s".formatted(length, length()));
		}

		// TODO assert inputs are more restrictive than current filter
		if (off == 0) {
			return new ByteSliceNoOffset(buffer, length);
		} else {
			return ByteSlice.builder().buffer(buffer).offset(off).length(length).build();
		}
	}

	// CPD-OFF
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
		return ByteSlice.equals(this, otherByteSlice);
	}

	@Override
	public byte read(int position) {
		return buffer[position];
	}
}