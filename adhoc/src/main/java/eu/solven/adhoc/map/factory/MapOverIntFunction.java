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

import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.IntSupplier;

import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Represents an {@link IAdhocMap} given an {@link IntFunction} representing dictionarized value.
 * 
 * @author Benoit Lacelle
 * 
 */
public class MapOverIntFunction extends AbstractAdhocMap {

	@NonNull
	final IntFunction<Object> sequencedValues;

	@Builder
	public MapOverIntFunction(ISliceFactory factory, SequencedSetLikeList keys, IntFunction<Object> unorderedValues) {
		super(factory, keys);
		this.sequencedValues = unorderedValues;
	}

	@Builder(builderMethodName = "builderCustomHashcode")
	public MapOverIntFunction(ISliceFactory factory,
			SequencedSetLikeList keys,
			IntFunction<Object> unorderedValues,
			IntSupplier hashcodeSupplier) {
		super(factory, keys, hashcodeSupplier);
		this.sequencedValues = unorderedValues;
	}

	@Override
	protected Object getSequencedValueRaw(int index) {
		return sequencedValues.apply(index);
	}

	@Override
	protected Object getSortedValueRaw(int index) {
		return getSequencedValueRaw(sequencedKeys.unorderedIndex(index));
	}

	@RequiredArgsConstructor
	final class RetainedIntFunction implements IntFunction<Object> {
		final int[] sequencedIndexes;

		@Override
		public Object apply(int index) {
			return sequencedValues.apply(sequencedIndexes[index]);
		}

		@SuppressWarnings("PMD.UseVarargs")
		public IntFunction<Object> retain(int[] retainedIndexes) {
			// TODO Could we unroll the double de-reference from the cache?
			// It would prevent deep retainAll chains into deep de-reference chains
			// Need micro-benchmark
			// return retainedIndex -> this.apply(retainedIndexes[retainedIndex]);
			return retainedIndex -> sequencedValues.apply(sequencedIndexes[retainedIndexes[retainedIndex]]);
		}

	}

	@Override
	public IAdhocMap retainAll(Set<String> retainedColumns) {
		RetainedKeySet retainedKeyset = retainKeyset(retainedColumns);

		int[] sequencedIndexes = retainedKeyset.getSequencedIndexes();
		IntFunction<Object> retainedSequencedValues;
		if (sequencedValues instanceof RetainedIntFunction retainedIntFunction) {
			retainedSequencedValues = retainedIntFunction.retain(sequencedIndexes);
		} else {
			retainedSequencedValues = new RetainedIntFunction(sequencedIndexes);
		}

		// compute hashCode differentially based on excluded entries
		// This is expected to be faster as we expect the parent map to be hashed at least once if the retained map is
		// hashed
		IntSupplier retainedHashcode = () -> {
			int excludedhashcode = 0;

			@NonNull
			int[] excludedColumn = retainedKeyset.getExcludedIndexes();

			for (int excludedColumnIndex : excludedColumn) {
				String key = sequencedKeys.getKey(excludedColumnIndex);
				Object value = getSequencedValue(excludedColumnIndex);
				// see `Map.Entry#hashCode`
				excludedhashcode += Objects.hashCode(key) ^ Objects.hashCode(value);
			}

			return this.hashCode() - excludedhashcode;
		};

		return new MapOverIntFunction(getFactory(),
				retainedKeyset.getKeys(),
				retainedSequencedValues,
				retainedHashcode);
	}
}