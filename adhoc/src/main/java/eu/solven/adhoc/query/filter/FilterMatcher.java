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
package eu.solven.adhoc.query.filter;

import java.util.Map;
import java.util.function.Predicate;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Defines parameters configuring for {@link MoreFilterHelpers#match}
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class FilterMatcher {

	/**
	 * When matching a {@link IColumnFilter} but the input misses given column.
	 * 
	 * By default, we behave as if the value was null if {@link IColumnFilter#isNullIfAbsent()}. Else we consider the
	 * entry does not match (e.g. `color=blue` should not matcher `country=FR`).
	 */
	private static final Predicate<IColumnFilter> IF_MISSING_AS_NULL = filterMissingColumn -> {
		if (filterMissingColumn.isNullIfAbsent()) {
			log.trace("Treat absent as null");
			return filterMissingColumn.getValueMatcher().match(null);
		} else {
			log.trace("Do not treat absent as null, but as missing, hence not matched");
			return false;
		}
	};

	@Default
	@NonNull
	ITableTranscoder transcoder = ITableTranscoder.identity();

	@NonNull
	ISliceFilter filter;

	@Default
	@NonNull
	Predicate<IColumnFilter> onMissingColumn = IF_MISSING_AS_NULL;

	public boolean match(Map<String, ?> map) {
		return MoreFilterHelpers.match(transcoder, filter, onMissingColumn, map);
	}

	public boolean match(ITabularRecord tabularRecord) {
		return MoreFilterHelpers.match(transcoder, filter, onMissingColumn, tabularRecord);
	}
}
