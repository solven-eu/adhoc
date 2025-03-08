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
package eu.solven.adhoc.record;

import java.util.List;
import java.util.stream.IntStream;

import eu.solven.adhoc.slice.IAdhocSlice;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.IValueConsumer;

/**
 * Used to provide the values given a {@link List} of {@link ISliceToValue} and an {@link IAdhocSlice}
 * 
 * @author Benoit Lacelle
 */
public interface ISlicedRecord {
	/**
	 * 
	 * @return true if the List of {@link ISliceToValue} is empty.
	 */
	boolean isEmpty();

	/**
	 * @return the number of {@link ISliceToValue} being considered.
	 */
	int size();

	/**
	 * 
	 * @param index
	 *            from 0 to `.size()` excluded
	 * @param valueConsumer
	 *            some {@link IValueConsumer} to receive the data.
	 */
	void read(int index, IValueConsumer valueConsumer);

	@Deprecated(since = "Prefer `void read(int index, IValueConsumer valueConsumer)`")
	default List<?> asList() {
		return IntStream.range(0, size()).mapToObj(index -> IValueConsumer.getValue(vc -> read(index, vc))).toList();
	}

	/**
	 * 
	 * @param array
	 *            an array to be written. If too small, we write only the first indexes. If too large, we write only the
	 *            first indexes.
	 */
	@Deprecated(since = "Prefer `void read(int index, IValueConsumer valueConsumer)`")
	default void asArray(Object[] array) {
		for (int i = 0; i < Math.min(array.length, size()); i++) {
			int finalI = i;
			array[i] = IValueConsumer.getValue(vc -> read(finalI, vc));
		}
	}
}
