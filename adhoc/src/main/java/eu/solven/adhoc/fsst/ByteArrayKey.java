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
package eu.solven.adhoc.fsst;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * Value object for using byte[] slices as HashMap keys.
 * 
 * @author Benoit Lacelle
 */
final class ByteArrayKey {

	private final byte[] data;
	private final int offset;
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private final int length;
	private final int hash;

	/**
	 * Should not be used, out of through `.read` and `.write`.
	 * 
	 * @param source
	 * @param offset
	 * @param length
	 */
	@SuppressWarnings("PMD.ArrayIsStoredDirectly")
	ByteArrayKey(byte[] source, int offset, int length) {
		this.data = source;
		this.offset = offset;
		this.length = length;
		this.hash = hashCode(this.data, offset, length);
	}

	// BEWARE Is this actually relevant to unroll for performance?
	@SuppressWarnings({ "checkstyle.MagicNumber", "PMD.AssignmentInOperand" })
	static int hashCode(byte[] a, int offset, int length) {
		int h = 1;
		int i = offset;
		int end = offset + length;

		while (i + 3 < end) {
			h = 31 * h + a[i++];
			h = 31 * h + a[i++];
			h = 31 * h + a[i++];
			h = 31 * h + a[i++];
		}
		while (i < end) {
			h = 31 * h + a[i++];
		}
		return h;
	}

	/**
	 * Make a ByteArrayKey for a read operation: the source may be referred and modified later
	 * 
	 * @param source
	 * @param offset
	 * @param length
	 * @return
	 */
	static ByteArrayKey read(byte[] source, int offset, int length) {
		return new ByteArrayKey(source, offset, length);
	}

	/**
	 * Make a ByteArrayKey for a write operation: the data is copied in a fresh `byte[]` as the input source may
	 * modified later while this object has to be immutable.
	 * 
	 * @param source
	 * @param offset
	 * @param length
	 * @return
	 */
	static ByteArrayKey write(byte[] source, int offset, int length) {
		return new ByteArrayKey(Arrays.copyOfRange(source, offset, offset + length), 0, length);
	}

	int length() {
		return length;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ByteArrayKey other)) {
			return false;
		}

		if (this.length != other.length) {
			return false;
		}
		for (int i = 0; i < length; i++) {
			if (this.data[this.offset + i] != other.data[other.offset + i]) {
				return false;
			}
		}

		return true;
	}

	@Override
	public int hashCode() {
		return hash;
	}

	void writeBytes(ByteArrayOutputStream out) {
		out.write(data, offset, length);
	}

	@Override
	public String toString() {
		return toHexString(data, offset, length);
	}

	static String toHexString(byte[] a, int offset, int length) {
		Objects.checkFromIndexSize(offset, length, a.length);

		StringBuilder sb = new StringBuilder(length * 2);
		for (int i = offset; i < offset + length; i++) {
			sb.append(String.format("%02x", a[i]));
		}
		return sb.toString();
	}
}