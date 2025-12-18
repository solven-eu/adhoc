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
package eu.solven.adhoc.dictionary;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import eu.solven.adhoc.map.factory.NavigableSetLikeList;

/**
 * Extends {@link ListDictionarizerFactory} by relying on a `int[][]`. The memory is referenced for the whole column,
 * but it enable more compact representation by sparing the `int[]` overhead in the row-based.
 * 
 * @author Benoit Lacelle
 */
public class ColumnarListDictionarizerFactory extends ListDictionarizerFactory {
	final int size;

	public ColumnarListDictionarizerFactory(int size) {
		this.size = size;
	}

	@Override
	public IListDictionarizer makeDictionarizer(NavigableSetLikeList keySet) {
		int width = keySet.size();
		int[][] table = IntStream.range(0, size).mapToObj(s -> new int[width]).toArray(int[][]::new);

		AtomicInteger rowIndex = new AtomicInteger();

		return new ListDictionarizerFactory.RowListDictionarizer(keySet) {
			@Override
			protected IntFunction<Object> toDictionarizedRow(NavigableSetLikeList keySet, List<Object> list) {
				int currentRowIndex = rowIndex.getAndIncrement();

				return extracted(keySet, list, new IIntArray() {

					@Override
					public int length() {
						return width;
					}

					@Override
					public void writeInt(int index, int value) {
						table[currentRowIndex][index] = value;
					}

					@Override
					public int readInt(int index) {
						return table[currentRowIndex][index];
					}
				});
			}
		};
	}
}
