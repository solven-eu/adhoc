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

import java.util.List;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.query.filter.value.RegexMatcher;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManagerSimple;
import eu.solven.pepper.core.PepperLogHelper;

public class MoreFilterHelpers {

	public static IValueMatcher transcodeType(ICustomTypeManagerSimple customTypeManager,
			String column,
			IValueMatcher valueMatcher) {

		if (!customTypeManager.mayTranscode(column)) {
			return valueMatcher;
		}

		if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
			return EqualsMatcher.isEqualTo(customTypeManager.toTable(column, equalsMatcher.getOperand()));
		} else if (valueMatcher instanceof InMatcher inMatcher) {
			List<Object> transcodedOperands = inMatcher.getOperands()
					.stream()
					.map(operand -> customTypeManager.toTable(column, operand))
					.toList();

			return InMatcher.isIn(transcodedOperands);
		} else if (valueMatcher instanceof NotMatcher notMatcher) {
			return NotMatcher.builder()
					.negated(transcodeType(customTypeManager, column, notMatcher.getNegated()))
					.build();
		} else if (valueMatcher instanceof NullMatcher || valueMatcher instanceof LikeMatcher
				|| valueMatcher instanceof RegexMatcher) {
			return valueMatcher;
		} else if (valueMatcher instanceof AndMatcher andMatcher) {
			List<IValueMatcher> transcoded = andMatcher.getOperands()
					.stream()
					.map(operand -> transcodeType(customTypeManager, column, operand))
					.toList();
			return AndMatcher.and(transcoded);
		} else if (valueMatcher instanceof OrMatcher orMatcher) {
			List<IValueMatcher> transcoded = orMatcher.getOperands()
					.stream()
					.map(operand -> transcodeType(customTypeManager, column, operand))
					.toList();
			return OrMatcher.or(transcoded);
		} else {
			// For complex valueMatcher, the custom may have a custom way to convert it into a table IValueMatcher
			return customTypeManager.toTable(column, valueMatcher);
		}
	}

	public static IAdhocFilter transcodeFilter(ICustomTypeManagerSimple customTypeManager,
			ITableTranscoder tableTranscoder,
			IAdhocFilter filter) {

		if (filter.isMatchAll() || filter.isMatchNone()) {
			return filter;
		} else if (filter instanceof IColumnFilter columnFilter) {
			String column = columnFilter.getColumn();
			return ColumnFilter.builder()
					.column(tableTranscoder.underlyingNonNull(column))
					.valueMatcher(transcodeType(customTypeManager, column, columnFilter.getValueMatcher()))
					.build();
		} else if (filter instanceof IAndFilter andFilter) {
			return AndFilter.and(andFilter.getOperands()
					.stream()
					.map(operand -> transcodeFilter(customTypeManager, tableTranscoder, operand))
					.toList());
		} else if (filter instanceof IOrFilter orFilter) {
			return OrFilter.or(orFilter.getOperands()
					.stream()
					.map(operand -> transcodeFilter(customTypeManager, tableTranscoder, operand))
					.toList());
		} else if (filter instanceof INotFilter notFilter) {
			return NotFilter.not(transcodeFilter(customTypeManager, tableTranscoder, notFilter.getNegated()));
		} else {
			throw new UnsupportedOperationException(
					"Not managed: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}
}
