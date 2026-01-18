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
package eu.solven.adhoc.compression.dictionary;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;

import eu.solven.adhoc.compression.MapDictionarizer;
import eu.solven.adhoc.compression.packing.PackedIntegers;
import eu.solven.adhoc.compression.page.IReadableColumn;
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
	List<Object> distinctValues;

	@NonNull
	IntUnaryOperator rowToDic;

	@Override
	public Object readValue(int rowIndex) {
		return distinctValues.get(rowToDic.applyAsInt(rowIndex));
	}

	public static IReadableColumn fromArray(List<?> asArray) {
		List<Object> intToObject = new ArrayList<>();
		MapDictionarizer dictionarizer = MapDictionarizer.builder().intToObject(intToObject).build();

		int[] rowToDic = new int[asArray.size()];
		for (int i = 0; i < asArray.size(); i++) {
			Object rawValue = asArray.get(i);
			rowToDic[i] = dictionarizer.toInt(rawValue);
		}

		// Given rowToDic holds small integers, it is relevant to pack it for compression purposes
		PackedIntegers packedIntegers = PackedIntegers.doPack(rowToDic);

		return DictionarizedObjectColumn.builder()
				.distinctValues(intToObject)
				.rowToDic(packedIntegers::readInt)
				.build();
	}

}
