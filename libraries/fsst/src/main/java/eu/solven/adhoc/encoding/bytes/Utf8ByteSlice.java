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

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonValue;

import lombok.Builder;

/**
 * Wraps an {@link IByteSlice} while marking it explicitly as UTF-8.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class Utf8ByteSlice implements IByteSlice {
	final IByteSlice byteSlice;

	@Override
	public boolean isFastCrop() {
		return byteSlice.isFastCrop();
	}

	@Override
	public byte[] crop() {
		return byteSlice.crop();
	}

	@Override
	public byte read(int position) {
		return byteSlice.read(position);
	}

	@Override
	public byte[] buffer() {
		return byteSlice.buffer();
	}

	@Override
	public int length() {
		return byteSlice.length();
	}

	@Override
	public int offset() {
		return byteSlice.offset();
	}

	@Override
	public IByteSlice sub(int off, int length) {
		return byteSlice.sub(off, length);
	}

	@Override
	@JsonValue
	public String toString() {
		return asString(StandardCharsets.UTF_8);
	}

	/**
	 * Equality is content-based, delegating to the wrapped {@link IByteSlice} (compares the underlying UTF-8 bytes).
	 *
	 * <p>
	 * Two {@link Utf8ByteSlice} are equal iff their wrapped {@link IByteSlice} report the same byte sequence — which is
	 * also the contract of {@link IByteSlice#equals(Object)}. They are NOT equal to a {@link String} carrying the same
	 * characters: a {@link Utf8ByteSlice} is a byte-level value, not a UTF-16 char value.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof Utf8ByteSlice other)) {
			return false;
		}
		return byteSlice.equals(other.byteSlice);
	}

	/**
	 * Hash is delegated to the wrapped {@link IByteSlice} (computed from the underlying byte sequence using the
	 * {@code 31 * h + b} loop seeded at {@code 1}).
	 *
	 * <p>
	 * This is intentionally distinct from {@link String#hashCode()} (which iterates over UTF-16 code units, seeded at
	 * {@code 0}). A {@link Utf8ByteSlice} MUST NOT be used as a key against a {@link String}-keyed map. Any incidental
	 * collision with {@link String#hashCode()} would be opportunistic and is NOT part of the contract.
	 */
	@Override
	public int hashCode() {
		return byteSlice.hashCode();
	}

	public static Utf8ByteSlice fromString(String string) {
		return Utf8ByteSlice.builder().byteSlice(IByteSlice.wrap(string.getBytes(StandardCharsets.UTF_8))).build();
	}
}
