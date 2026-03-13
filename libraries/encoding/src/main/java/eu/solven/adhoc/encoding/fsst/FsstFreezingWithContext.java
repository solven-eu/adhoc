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
package eu.solven.adhoc.encoding.fsst;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;

import eu.solven.adhoc.encoding.bytes.IByteSlice;
import eu.solven.adhoc.encoding.bytes.Utf8ByteSlice;
import eu.solven.adhoc.encoding.column.IAppendableColumn;
import eu.solven.adhoc.encoding.column.IReadableColumn;
import eu.solven.adhoc.encoding.column.ObjectArrayColumn;
import eu.solven.adhoc.encoding.column.freezer.FreezerHelpers;
import eu.solven.adhoc.encoding.column.freezer.FsstReadableColumn;
import eu.solven.adhoc.encoding.column.freezer.IFreezingWithContext;
import eu.solven.adhoc.encoding.column.freezer.SynchronousFreezingStrategy;

/**
 * Extends {@link SynchronousFreezingStrategy} by enabling FSST over columns of {@link String}.
 * 
 * @author Benoit Lacelle
 * 
 */
public class FsstFreezingWithContext implements IFreezingWithContext {
	private static final Set<Class<? extends Object>> FREEZABLE_CLASSES =
			Sets.newLinkedHashSet(Arrays.asList(String.class, Utf8ByteSlice.class, null));

	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	@Override
	public Optional<IReadableColumn> freeze(IAppendableColumn column, Map<String, Object> freezingContext) {
		if (column instanceof ObjectArrayColumn arrayColumn) {
			List<?> array = arrayColumn.getAsArray();

			Set<?> classes = FreezerHelpers.classesWithContext(freezingContext, array);

			if (Sets.difference(classes, FREEZABLE_CLASSES).isEmpty()) {
				List<IByteSlice> primitiveArray = array.stream().map(this::toByteSlice).toList();
				return Optional.of(readableColumn(primitiveArray));
			}
		}

		return Optional.empty();
	}

	protected IByteSlice toByteSlice(Object s) {
		if (s == null || s instanceof Utf8ByteSlice) {
			return (IByteSlice) s;
		}
		return IByteSlice.wrap(s.toString().getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Given the input, we train an FSST model and encode each String with given model.
	 * 
	 * @param primitiveArray
	 * @return an FSST-encoded {@link IReadableColumn}
	 */
	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	protected IReadableColumn readableColumn(List<IByteSlice> primitiveArray) {
		SymbolTable table = FsstTrainer.builder().build().train(primitiveArray);

		List<IByteSlice> encoded =
				primitiveArray.stream().map(bytes -> bytes == null ? null : table.encodeAll(bytes)).toList();

		// BEWARE This will drop the trained symbols, keeping only decoded capacities
		return FsstReadableColumn.builder().decoder(table.getDecoding()).encoded(encoded).build();
	}

}
