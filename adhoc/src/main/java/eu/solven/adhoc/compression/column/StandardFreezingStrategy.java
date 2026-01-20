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
package eu.solven.adhoc.compression.column;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import eu.solven.adhoc.compression.column.freezer.AdhocFreezingUnsafe;
import eu.solven.adhoc.compression.column.freezer.IFreezingStrategy;
import eu.solven.adhoc.compression.column.freezer.IFreezingWithContext;
import eu.solven.adhoc.compression.page.IReadableColumn;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * Standard {@link IFreezingStrategy}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
public class StandardFreezingStrategy implements IFreezingStrategy {

	@Default
	@NonNull
	List<IFreezingWithContext> freezersWithContext = AdhocFreezingUnsafe.getFreezers();

	// TODO This computation could be done asynchronously
	@Override
	public IReadableColumn freeze(IAppendableColumn column) {
		if (column instanceof ObjectArrayColumn arrayColumn) {
			Map<String, Object> freezingContext = new LinkedHashMap<>();

			Optional<IReadableColumn> output = Optional.empty();
			for (IFreezingWithContext freezer : freezersWithContext) {
				output = freezer.freeze(arrayColumn, freezingContext);

				if (!output.isEmpty()) {
					break;
				}
			}

			// TODO wrap in unmodifiable?
			return output.orElse(column);
		} else {
			// TODO wrap in unmodifiable?
			return column;
		}
	}

}
