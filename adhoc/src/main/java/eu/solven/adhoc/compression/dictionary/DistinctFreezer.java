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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.math.LongMath;

import eu.solven.adhoc.compression.column.IAppendableColumn;
import eu.solven.adhoc.compression.column.ObjectArrayColumn;
import eu.solven.adhoc.compression.column.freezer.IFreezingWithContext;
import eu.solven.adhoc.compression.page.IReadableColumn;

/**
 * {@link IFreezingWithContext} which will enable dictionarization.
 * 
 * @author Benoit Lacelle
 */
public final class DistinctFreezer implements IFreezingWithContext {
	private static final int DISTINCT_FACTOR = 16;

	@Override
	public Optional<IReadableColumn> freeze(IAppendableColumn column, Map<String, Object> freezingContext) {
		if (column instanceof ObjectArrayColumn arrayColumn) {
			List<?> array = arrayColumn.getAsArray();

			long countDistinct = (long) freezingContext.computeIfAbsent("count_distinct", k -> {
				return array.stream().distinct().count();
			});

			if (LongMath.saturatedMultiply(countDistinct, DISTINCT_FACTOR) <= array.size()) {
				return Optional.of(DictionarizedObjectColumn.fromArray(array));
			} else {
				return Optional.empty();
			}
		} else {
			return Optional.empty();
		}
	}
}