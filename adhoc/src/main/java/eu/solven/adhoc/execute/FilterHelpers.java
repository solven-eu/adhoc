/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.execute;

import java.util.Map;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import eu.solven.adhoc.api.v1.filters.INotFilter;
import eu.solven.adhoc.api.v1.filters.IOrFilter;
import eu.solven.adhoc.database.transcoder.IAdhocTableTranscoder;
import eu.solven.adhoc.database.transcoder.IdentityTranscoder;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FilterHelpers {

	public static boolean match(IAdhocFilter filter, Map<String, ?> input) {
		return match(new IdentityTranscoder(), filter, input);
	}

	public static boolean match(IAdhocTableTranscoder transcoder, IAdhocFilter filter, Map<String, ?> input) {
		if (filter.isAnd()) {
			IAndFilter andFilter = (IAndFilter) filter;
			return andFilter.getOperands().stream().allMatch(f -> match(f, input));
		} else if (filter.isOr()) {
			IOrFilter orFilter = (IOrFilter) filter;
			return orFilter.getOperands().stream().anyMatch(f -> match(f, input));
		} else if (filter.isColumnFilter()) {
			IColumnFilter columnFilter = (IColumnFilter) filter;

			String underlyingColumn = transcoder.underlying(columnFilter.getColumn());
			Object value = input.get(underlyingColumn);

			if (value == null) {
				if (input.containsKey(underlyingColumn)) {
					log.trace("Key to null-ref");
				} else {
					log.trace("Missing key");
					if (columnFilter.isNullIfAbsent()) {
						log.trace("Treat absent as null");
					} else {
						log.trace("Do not treat absent as null, but as missing hence not acceptable");
						return false;
					}
				}
			}

			return columnFilter.getValueMatcher().match(value);
		} else if (filter.isNot()) {
			INotFilter notFilter = (INotFilter) filter;
			return !match(notFilter.getNegated(), input);
		} else {
			throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(filter).toString());
		}
	}
}
