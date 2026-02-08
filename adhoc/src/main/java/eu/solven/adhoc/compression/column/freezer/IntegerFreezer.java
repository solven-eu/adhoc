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
package eu.solven.adhoc.compression.column.freezer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.compression.column.IAppendableColumn;
import eu.solven.adhoc.compression.column.IntegerArrayColumn;
import eu.solven.adhoc.compression.column.ObjectArrayColumn;
import eu.solven.adhoc.compression.page.IReadableColumn;

/**
 * {@link IFreezingWithContext} when all values are {@link Integer}.
 * 
 * @author Benoit Lacelle
 */
public final class IntegerFreezer implements IFreezingWithContext {
	@Override
	public Optional<IReadableColumn> freeze(IAppendableColumn column, Map<String, Object> freezingContext) {
		if (column instanceof ObjectArrayColumn arrayColumn) {
			List<?> array = arrayColumn.getAsArray();

			Set<?> classes = LongFreezer.classesWithContext(freezingContext, array);

			if (classes.size() == 1 && classes.contains(Integer.class)) {
				int[] primitiveArray = array.stream().mapToInt(Integer.class::cast).toArray();
				return Optional.of(IntegerArrayColumn.builder().asArray(primitiveArray).build());
			} else {
				return Optional.empty();
			}
		} else {
			return Optional.empty();
		}
	}

}