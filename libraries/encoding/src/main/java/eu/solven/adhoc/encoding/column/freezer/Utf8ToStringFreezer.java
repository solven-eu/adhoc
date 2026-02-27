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
package eu.solven.adhoc.encoding.column.freezer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.encoding.bytes.Utf8ByteSlice;
import eu.solven.adhoc.encoding.column.IAppendableColumn;
import eu.solven.adhoc.encoding.column.IReadableColumn;
import eu.solven.adhoc.encoding.column.ObjectArrayColumn;
import eu.solven.adhoc.encoding.fsst.FsstFreezingWithContext;

/**
 * Fallback {@link IFreezingWithContext} that normalises {@link AdhocUtf8} values to {@link String}.
 *
 * <p>
 * {@link FsstFreezingWithContext} consumes columns whose values are exclusively {@link AdhocUtf8} or {@link String}.
 * When the column is mixed (e.g. {@link AdhocUtf8} alongside numeric types), FSST declines and this freezer converts
 * each {@link AdhocUtf8} to a {@link String} so that downstream code never observes raw {@link AdhocUtf8} instances.
 *
 * <p>
 * This freezer should be registered <em>after</em> {@link FsstFreezingWithContext} in the freezer chain so that the
 * FSST path still fires when applicable.
 *
 * @author Benoit Lacelle
 */
public final class Utf8ToStringFreezer implements IFreezingWithContext {

	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	@Override
	public Optional<IReadableColumn> freeze(IAppendableColumn column, Map<String, Object> freezingContext) {
		if (!(column instanceof ObjectArrayColumn arrayColumn)) {
			return Optional.empty();
		}

		List<?> array = arrayColumn.getAsArray();

		Set<?> classes = FreezerHelpers.classesWithContext(freezingContext, array);

		if (classes.contains(Utf8ByteSlice.class)) {
			List<Object> normalized = new ArrayList<>(array.size());
			for (Object v : array) {
				normalized.add(v instanceof Utf8ByteSlice u ? u.toString() : v);
			}
			return Optional.of(ObjectArrayColumn.builder().asArray(normalized).build());
		} else {
			return Optional.empty();
		}
	}

}
