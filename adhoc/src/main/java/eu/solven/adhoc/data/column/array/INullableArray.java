/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.data.column.array;

import java.util.stream.IntStream;

/**
 * Adds `nullability` to an array-like structure. By `array-like`, we refer to a structure we can grow but not shrink, a
 * `set` can be called on any index, `remove` is a set-to-null.
 * 
 * @author Benoit Lacelle
 */
public interface INullableArray {

	boolean isNull(int index);

	/**
	 * 
	 * @return the number of nun-null elements
	 */
	int sizeNotNull();

	/**
	 * 
	 * @param index
	 * @return true if there is a non-null value at given index.
	 */
	boolean containsIndex(int index);

	/**
	 * 
	 * @return an {@link IntStream} of the indexes associated to a nun-null value
	 */
	IntStream indexStream();

	boolean addNull();

}
