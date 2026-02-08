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
package eu.solven.adhoc.fsst;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.compression.column.IAppendableColumn;
import eu.solven.adhoc.compression.column.ObjectArrayColumn;
import eu.solven.adhoc.compression.column.freezer.IFreezingWithContext;
import eu.solven.adhoc.compression.column.freezer.LongFreezer;
import eu.solven.adhoc.compression.column.freezer.SynchronousFreezingStrategy;
import eu.solven.adhoc.compression.page.IReadableColumn;

/**
 * Extends {@link SynchronousFreezingStrategy} by enabling FSST over columns of {@link String}.
 * 
 * @author Benoit Lacelle
 * 
 */
public class FsstFreezingWithContext implements IFreezingWithContext {

	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	@Override
	public Optional<IReadableColumn> freeze(IAppendableColumn column, Map<String, Object> freezingContext) {
		if (column instanceof ObjectArrayColumn arrayColumn) {
			List<?> array = arrayColumn.getAsArray();

			Set<?> classes = LongFreezer.classesWithContext(freezingContext, array);

			if (classes.size() == 1 && classes.contains(String.class)
					|| classes.size() == 2 && classes.contains(String.class) && classes.contains(null)) {
				List<byte[]> primitiveArray = array.stream()
						.map(s -> s == null ? null : s.toString().getBytes(StandardCharsets.UTF_8))
						.toList();
				return Optional.of(readableColumn(primitiveArray));
			}
		}

		return Optional.empty();
	}

	/**
	 * Given the input, we train an FSST model and encode each String with given model.
	 * 
	 * @param primitiveArray
	 * @return an FSST-encoded {@link IReadableColumn}
	 */
	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	protected IReadableColumn readableColumn(List<byte[]> primitiveArray) {
		SymbolTable table = FsstTrainer.train(primitiveArray.iterator());

		FsstEncoder enc = new FsstEncoder(table);

		List<byte[]> encoded = primitiveArray.stream().map(bytes -> bytes == null ? null : enc.encode(bytes)).toList();

		FsstDecoder decoder = new FsstDecoder(table);
		return FsstReadableColumn.builder().decoder(decoder).encoded(encoded).build();
	}

}
