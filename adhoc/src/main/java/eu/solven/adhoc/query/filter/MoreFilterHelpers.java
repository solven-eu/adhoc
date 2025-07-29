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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import eu.solven.adhoc.data.row.ITabularRecord;
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
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Helpers methods around {@link ISliceFilter} and {@link IValueMatcher}. This includes those which does not fit into
 * {@link FilterHelpers}, due to dependency to `!public` dependencies.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
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

	public static ISliceFilter transcodeFilter(ICustomTypeManagerSimple customTypeManager,
			ITableTranscoder tableTranscoder,
			ISliceFilter filter) {

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

	/**
	 * 
	 * @param filter
	 * @param input
	 * @return true if the input matches the filter
	 */
	public static boolean match(ISliceFilter filter, Map<String, ?> input) {
		return FilterMatcher.builder().filter(filter).build().match(input);
	}

	public static boolean match(ISliceFilter filter, String column, Object value) {
		return FilterMatcher.builder().filter(filter).build().match(Collections.singletonMap(column, value));
	}

	public static boolean match(ITableTranscoder transcoder, ISliceFilter filter, Map<String, ?> input) {
		return FilterMatcher.builder().transcoder(transcoder).filter(filter).build().match(input);
	}

	/**
	 * 
	 * @return true if the input matches the filter, where each column in input is transcoded.
	 */
	@Deprecated(since = "Signature may be regularly enriched. Rely on `FilterParameters`")
	public static boolean match(ITableTranscoder transcoder,
			ISliceFilter filter,
			Predicate<IColumnFilter> onMissingColumn,
			Map<String, ?> input) {
		return FilterHelpers.visit(filter, new IFilterVisitor() {

			@Override
			public boolean testAndOperands(Set<? extends ISliceFilter> operands) {
				return operands.stream().allMatch(f -> match(transcoder, f, onMissingColumn, input));
			}

			@Override
			public boolean testOrOperands(Set<? extends ISliceFilter> operands) {
				return operands.stream().anyMatch(f -> match(transcoder, f, onMissingColumn, input));
			}

			@Override
			public boolean testColumnOperand(IColumnFilter columnFilter) {
				String underlyingColumn = transcoder.underlyingNonNull(columnFilter.getColumn());
				Object value = input.get(underlyingColumn);

				if (value == null) {
					if (input.containsKey(underlyingColumn)) {
						log.trace("Key to null-ref");
					} else {
						log.trace("Missing key");
						return onMissingColumn.test(columnFilter);
					}
				}

				return columnFilter.getValueMatcher().match(value);
			}

			@Override
			public boolean testNegatedOperand(ISliceFilter negated) {
				return !match(transcoder, negated, onMissingColumn, input);
			}

		});
	}

	public static boolean match(ISliceFilter filter, ITabularRecord input) {
		return FilterMatcher.builder().filter(filter).build().match(input);
	}

	public static boolean match(ITableTranscoder transcoder,
			ISliceFilter filter,
			Predicate<IColumnFilter> onMissingColumn,
			ITabularRecord input) {
		if (filter.isMatchAll()) {
			return true;
		} else if (filter.isMatchNone()) {
			return false;
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			return andFilter.getOperands().stream().allMatch(f -> match(transcoder, f, onMissingColumn, input));
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			return orFilter.getOperands().stream().anyMatch(f -> match(transcoder, f, onMissingColumn, input));
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			String underlyingColumn = transcoder.underlyingNonNull(columnFilter.getColumn());
			Object value = input.getGroupBy(underlyingColumn);

			if (value == null) {
				if (input.groupByKeySet().contains(underlyingColumn)) {
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
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			return !match(transcoder, notFilter.getNegated(), onMissingColumn, input);
		} else {
			throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(filter).toString());
		}
	}
}
