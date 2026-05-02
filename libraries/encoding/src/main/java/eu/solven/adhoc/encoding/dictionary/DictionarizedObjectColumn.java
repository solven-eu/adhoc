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
package eu.solven.adhoc.encoding.dictionary;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntUnaryOperator;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.encoding.IIntArray;
import eu.solven.adhoc.encoding.column.IReadableColumn;
import eu.solven.adhoc.encoding.column.ObjectArrayColumn;
import eu.solven.adhoc.encoding.column.freezer.AdhocFreezingUnsafe;
import eu.solven.adhoc.encoding.column.freezer.IFreezingWithContext;
import eu.solven.adhoc.encoding.packing.PackedIntegers;
import lombok.Builder;
import lombok.NonNull;

/**
 * An {@link IReadableColumn} based on a dictionary.
 *
 * @author Benoit Lacelle
 */
@Builder
public class DictionarizedObjectColumn implements IReadableColumn {

	@NonNull
	IReadableColumn distinctValues;

	@NonNull
	IntUnaryOperator rowToDic;

	@Override
	public @Nullable Object readValue(int rowIndex) {
		int dictionarizedIndex = rowToDic.applyAsInt(rowIndex);
		return distinctValues.readValue(dictionarizedIndex);
	}

	public static IReadableColumn fromArray(List<?> asList) {
		return fromArray(asList, List.of());
	}

	/**
	 * Build a {@link DictionarizedObjectColumn} and apply the given freezer chain to the (small) dictionary of distinct
	 * values, so that downstream freezing strategies (e.g. {@code Utf8ToStringFreezer},
	 * {@code FsstFreezingWithContext}) get a chance to normalise/compress the dictionary entries even when
	 * {@link DistinctFreezer} fired first on the surrounding column.
	 *
	 * @param asList
	 *            the original (non-distinct) values
	 * @param dictionaryFreezers
	 *            freezer chain to apply to the dictionary of distinct values; pass an empty list to keep the dictionary
	 *            untouched (raw {@link ObjectArrayColumn})
	 * @return a {@link DictionarizedObjectColumn} backed by the (possibly further-frozen) dictionary
	 */
	public static IReadableColumn fromArray(List<?> asList, List<IFreezingWithContext> dictionaryFreezers) {
		List<Object> intToObject = new ArrayList<>();
		MapDictionarizer dictionarizer = MapDictionarizer.builder().intToObject(intToObject).build();

		int size = asList.size();
		int[] rowToDic = new int[size];
		for (int i = 0; i < size; i++) {
			Object rawValue = asList.get(i);
			rowToDic[i] = dictionarizer.toInt(rawValue);
		}

		// Given rowToDic holds small integers, it is relevant to pack it for compression purposes
		IIntArray packedIntegers = PackedIntegers.doPack(rowToDic);

		if (AdhocFreezingUnsafe.isCheckPostCompression()) {
			checkPostCompression(asList, dictionarizer, size, packedIntegers);
		}

		IReadableColumn distinctValuesColumn = freezeDictionary(intToObject, dictionaryFreezers);

		return DictionarizedObjectColumn.builder()
				.distinctValues(distinctValuesColumn)
				.rowToDic(packedIntegers::readInt)
				.build();
	}

	/**
	 * Wraps the (small) dictionary into an {@link ObjectArrayColumn} and runs it through the given freezer chain,
	 * stopping at the first freezer that produces a non-empty {@link IReadableColumn}. If none fires (or the chain is
	 * empty), the dictionary is exposed as a plain {@link ObjectArrayColumn}.
	 *
	 * @param intToObject
	 *            the dictionary of distinct values, in dictionarisation order
	 * @param dictionaryFreezers
	 *            freezer chain to attempt
	 * @return an {@link IReadableColumn} backing the dictionary
	 */
	private static IReadableColumn freezeDictionary(List<Object> intToObject,
			List<IFreezingWithContext> dictionaryFreezers) {
		ObjectArrayColumn dictionary = ObjectArrayColumn.builder().build();
		for (Object distinct : intToObject) {
			dictionary.append(distinct);
		}
		Map<String, Object> ctx = new LinkedHashMap<>();
		for (IFreezingWithContext freezer : dictionaryFreezers) {
			Optional<IReadableColumn> output = freezer.freeze(dictionary, ctx);
			if (output.isPresent()) {
				return output.get();
			}
		}
		return dictionary;
	}

	static void checkPostCompression(List<?> asList,
			MapDictionarizer dictionarizer,
			int size,
			IIntArray packedIntegers) {
		if (packedIntegers.length() != size) {
			throw new IllegalStateException("Invalid length %s!=%s".formatted(packedIntegers.length(), size));
		}

		for (int i = 0; i < size; i++) {
			int fromPack = packedIntegers.readInt(i);
			Object fromDic = dictionarizer.fromInt(fromPack);
			Object fromList = asList.get(i);
			if (!Objects.equals(fromList, fromDic)) {
				throw new IllegalStateException("Invalid value as index=%s %s!=%s".formatted(i, fromList, fromDic));
			}
		}
	}

}
