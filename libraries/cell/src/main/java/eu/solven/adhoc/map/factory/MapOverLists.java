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
package eu.solven.adhoc.map.factory;

import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import lombok.Builder;
import lombok.NonNull;

/**
 * A {@link Map} based on {@link SequencedSetLikeList} and values as a {@link List}.
 *
 * @author Benoit Lacelle
 */
public class MapOverLists extends AbstractAdhocMap {

	@NonNull
	final List<?> sequencedValues;

	@Builder
	public MapOverLists(ISliceFactory factory, SequencedSetLikeList keys, ImmutableList<?> sequencedValues) {
		super(factory, keys);
		this.sequencedValues = sequencedValues;
	}

	protected MapOverLists(ISliceFactory factory, SequencedSetLikeList keys, List<?> sequencedValues) {
		super(factory, keys);
		this.sequencedValues = sequencedValues;
	}

	@Override
	protected Object getSequencedValueRaw(int index) {
		return sequencedValues.get(index);
	}

	@Override
	protected Object getSortedValueRaw(int index) {
		return sequencedValues.get(sequencedKeys.unorderedIndex(index));
	}

	@Override
	public IAdhocMap retainAll(Set<String> retainedColumns) {
		RetainedKeySet retainedKeyset = retainKeyset(retainedColumns);

		int[] retainedIndexes = retainedKeyset.getSequencedIndexes();
		List<?> retainedSequencedValues = new AbstractList<>() {

			@Override
			public int size() {
				return retainedColumns.size();
			}

			@Override
			public Object get(int index) {
				int originalIndex = retainedIndexes[index];
				if (originalIndex == -1) {
					// retained a not present column
					return null;
				} else {
					return sequencedValues.get(originalIndex);
				}
			}
		};

		return new MapOverLists(factory, retainedKeyset.getKeys(), retainedSequencedValues);
	}
}