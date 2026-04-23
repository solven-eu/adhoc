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
import java.util.function.IntSupplier;

import eu.solven.adhoc.cuboid.slice.SliceHelpers;
import eu.solven.adhoc.encoding.page.IInt2ObjectReader;
import eu.solven.adhoc.map.AbstractAdhocMap;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Represents an {@link IAdhocMap} given an {@link IInt2ObjectReader} representing dictionarized values.
 *
 * <p>
 * Uses the project-local {@link IInt2ObjectReader} interface rather than {@link java.util.function.IntFunction} so
 * domain types that already know how to read by {@code int} index (e.g.
 * {@code eu.solven.adhoc.encoding.page.ITableRowRead}) can be passed directly, avoiding the bound-method-reference
 * allocation (e.g. {@code row::readValue}) at call sites.
 *
 * @author Benoit Lacelle
 *
 */
public class MapOverIntFunction extends AbstractAdhocMap {

	@NonNull
	final IInt2ObjectReader sequencedValues;

	@Builder
	public MapOverIntFunction(ISliceFactory factory, SequencedSetLikeList keys, IInt2ObjectReader unorderedValues) {
		super(factory, keys);
		this.sequencedValues = unorderedValues;
	}

	@Builder(builderMethodName = "builderCustomHashcode")
	public MapOverIntFunction(ISliceFactory factory,
			SequencedSetLikeList keys,
			IInt2ObjectReader sequencedValues,
			IntSupplier hashcodeSupplier) {
		super(factory, keys, hashcodeSupplier);
		this.sequencedValues = sequencedValues;
	}

	@Override
	protected Object getSequencedValueRaw(int index) {
		return sequencedValues.read(index);
	}

	@Override
	protected Object getSortedValueRaw(int index) {
		return getSequencedValueRaw(sequencedKeys.unorderedIndex(index));
	}

	@RequiredArgsConstructor
	final class RetainedInt2ObjectReader implements IInt2ObjectReader {
		final int[] sequencedIndexes;

		@Override
		public Object read(int index) {
			return sequencedValues.read(sequencedIndexes[index]);
		}

		@SuppressWarnings("PMD.UseVarargs")
		IInt2ObjectReader retain(int[] retainedIndexes) {
			// TODO Could we unroll the double de-reference from the cache?
			// It would prevent deep retainAll chains into deep de-reference chains
			// Need micro-benchmark
			// return retainedIndex -> this.read(retainedIndexes[retainedIndex]);
			return retainedIndex -> sequencedValues.read(sequencedIndexes[retainedIndexes[retainedIndex]]);
		}

	}

	@Override
	public IAdhocMap retainAll(Set<String> retainedColumns) {
		if (retainedColumns.isEmpty()) {
			return SliceHelpers.grandTotal().asAdhocMap();
		}

		RetainedKeySet retainedKeyset = retainKeyset(retainedColumns);

		if (this.sequencedKeys.equals(retainedKeyset.getKeys())) {
			// In many cases, we retain all columns
			return this;
		}

		int[] sequencedIndexes = retainedKeyset.getSequencedIndexes();
		IInt2ObjectReader retainedSequencedValues;
		if (sequencedValues instanceof RetainedInt2ObjectReader retainedIntFunction) {
			retainedSequencedValues = retainedIntFunction.retain(sequencedIndexes);
		} else {
			retainedSequencedValues = new RetainedInt2ObjectReader(sequencedIndexes);
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