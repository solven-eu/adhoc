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
package eu.solven.adhoc.dictionary.page;

import java.util.List;

import lombok.Builder;
import lombok.NonNull;

/**
 * {@link IAppendableColumn} over a List.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class ObjectArrayColumn implements IAppendableColumn {

	@NonNull
	final List<Object> asArray;

	@Override
	public void append(Object normalizedValue) {
		asArray.add(normalizedValue);
	}

	@Override
	public Object readValue(int rowIndex) {
		return asArray.get(rowIndex);
	}

	@Override
	@SuppressWarnings("checkstyle:MagicNumber")
	public IReadableColumn freeze() {
		long countDistinct = asArray.stream().distinct().count();

		// TODO This computation could be done asynchronously
		if (countDistinct * 16 <= asArray.size()) {
			return DictionarizedObjectColumn.fromArray(asArray);
		} else if (asArray.stream().allMatch(Long.class::isInstance)) {
			long[] primitiveArray = asArray.stream().mapToLong(Long.class::cast).toArray();
			return LongArrayColumn.builder().asArray(primitiveArray).build();
		} else {
			return this;
		}

	}

}
