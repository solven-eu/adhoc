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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import eu.solven.adhoc.map.factory.NavigableSetLikeList;
import lombok.RequiredArgsConstructor;

/**
 * {@link IListDictionarizerFactory} with shared dictionaries.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class ListDictionarizerFactory implements IListDictionarizerFactory {
	// IDictionarizer are shared by the factory
	final Map<String, IDictionarizer> columnToDictionary = new ConcurrentHashMap<>();

	protected class RowListDictionarizer implements IListDictionarizer {
		private final NavigableSetLikeList keySet;

		public RowListDictionarizer(NavigableSetLikeList keySet) {
			this.keySet = keySet;
		}

		@Override
		public IntFunction<Object> dictionarize(List<Object> list) {
			return toDictionarizedRow(keySet, list);
		}

		/**
		 * The simplest dictionary approach: each {@link List} is converted into a int[].
		 * 
		 * @param keySet
		 * @param list
		 * @return
		 */
		protected IntFunction<Object> toDictionarizedRow(NavigableSetLikeList keySet, List<Object> list) {
			int[] indexTo = new int[list.size()];

			return extracted(keySet, list, new IIntArray() {

				@Override
				public int length() {
					return indexTo.length;
				}

				@Override
				public void writeInt(int index, int value) {
					indexTo[index] = value;
				}

				@Override
				public int readInt(int index) {
					return indexTo[index];
				}
			});
		}
	}

	@Override
	public IListDictionarizer makeDictionarizer(NavigableSetLikeList keySet) {
		return new RowListDictionarizer(keySet);
	}

	protected IDictionarizer getDictionarizer(String column) {
		return new MapDictionarizer();
	}

	protected IntFunction<Object> extracted(NavigableSetLikeList keySet, List<Object> list, IIntArray indexTo) {
		IntStream.range(0, indexTo.length()).forEach(index -> {
			IDictionarizer dictionary = columnToDictionary.get(keySet.getKey(index));

			indexTo.writeInt(index, dictionary.toInt(list.get(index)));
		});

		return index -> {
			int indexedValue = indexTo.readInt(index);

			IDictionarizer dictionary = columnToDictionary.get(keySet.getKey(index));

			return dictionary.fromInt(indexedValue);
		};
	}
}
