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
package eu.solven.adhoc.compression.packing;

import eu.solven.adhoc.compression.dictionary.IIntArray;

/**
 * Edge-case where the input holds only 0. Packing has not a single bit to consider.
 * 
 * @author Benoit Lacelle
 */
public final class ZeroPackedIntegers implements IIntArray {
	// number of packed ints
	int intsLength;

	ZeroPackedIntegers(int intsLength) {
		this.intsLength = intsLength;
	}

	@Override
	public int length() {
		return intsLength;
	}

	@Override
	public void writeInt(int index, int value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readInt(int index) {
		if (index < 0 || index >= intsLength) {
			throw new ArrayIndexOutOfBoundsException("index:%s >= length:%s".formatted(index, intsLength));
		}
		return 0;
	}

	@Override
	public String toString() {
		return FlexiblePackedIntegers.toString(this);
	}
}
