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
package eu.solven.adhoc.filter.editor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.Builder;
import lombok.Singular;

/**
 * A {@link IFilterEditor} based on a {@link Map} from edited columns to a forced value.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class SimpleFilterEditor implements IFilterEditor {
	public static final String KEY = "simple";

	public static final String P_SHIFTED = "shifted";

	@Singular
	Map<String, ?> columnToValues;

	@Override
	public IAdhocFilter editFilter(IAdhocFilter input) {
		AtomicReference<IAdhocFilter> edited = new AtomicReference<>(input);

		columnToValues.forEach((column, value) -> edited.set(shiftIfPresent(edited.get(), column, value)));

		return edited.get();
	}

	/**
	 *
	 * @param filter
	 *            the original filter
	 * @param column
	 *            the column to filter
	 * @param value
	 * @return a filter equivalent to input filter, except the column is filtered on given value
	 */
	public static IAdhocFilter shift(IAdhocFilter filter, String column, Object value) {
		if (filter.isMatchNone()) {
			return filter;
		} else if (filter.isMatchAll()) {
			return ColumnFilter.isEqualTo(column, value);
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			ColumnFilter shiftColumn = ColumnFilter.isEqualTo(column, value);
			if (columnFilter.getColumn().equals(column)) {
				// Replace the valueMatcher by the shift
				return shiftColumn;
			} else {
				// Combine both columns
				return AndFilter.and(columnFilter, shiftColumn);
			}
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			Set<IAdhocFilter> operands = andFilter.getOperands();

			List<IAdhocFilter> shiftedOperands = operands.stream().map(f -> shift(f, column, value)).toList();

			return AndFilter.and(shiftedOperands);
		} else {
			throw new NotYetImplementedException("filter=%s".formatted(filter));
		}
	}

	/**
	 *
	 * @param filter
	 *            the original filter
	 * @param column
	 *            the column to filter. If it is not already expressed, the filter is not shited.
	 * @param value
	 * @return like {@link #shift(String, Object, IAdhocFilter)} but only if the column is expressed
	 */
	public static IAdhocFilter shiftIfPresent(IAdhocFilter filter, String column, Object value) {
		IValueMatcher valueMatcher = FilterHelpers.getValueMatcher(filter, column);

		if (IValueMatcher.MATCH_ALL.equals(valueMatcher)) {
			// Do not shift if there is no filter
			return filter;
		} else {
			return shift(filter, column, value);
		}
	}

	/**
	 * 
	 * @param filter
	 * @param retainedColumns
	 * @return a filter where not retainedColumns columns are turned into `matchAll`
	 */
	public static IAdhocFilter retainsColumns(IAdhocFilter filter, Set<String> retainedColumns) {
		if (filter instanceof IColumnFilter columnFilter) {
			boolean isRetained = retainedColumns.contains(columnFilter.getColumn());

			if (isRetained) {
				return filter;
			} else {
				return IAdhocFilter.MATCH_ALL;
			}
		} else if (filter instanceof IAndFilter andFilter) {
			List<IAdhocFilter> unfilteredAnds = andFilter.getOperands()
					.stream()
					.map(subFilter -> retainsColumns(subFilter, retainedColumns))
					.toList();

			return AndFilter.and(unfilteredAnds);
		} else {
			throw new NotYetImplementedException("filter=%s".formatted(filter));
		}
	}

	/**
	 * 
	 * @param retainedColumns
	 * @return a {@link IFilterEditor} where not retainedColumns are turned into `matchAll`
	 */
	public static IFilterEditor retainsColumns(Set<String> retainedColumns) {
		return filter -> retainsColumns(filter, retainedColumns);
	}

	/**
	 * 
	 * @param suppressedColumns
	 * @return a filter where suppressedColumns columns are turned into `matchAll`
	 */
	public static IAdhocFilter suppressColumn(IAdhocFilter filter, Set<String> suppressedColumns) {
		if (filter instanceof IColumnFilter columnFilter) {
			boolean isSuppressed = suppressedColumns.contains(columnFilter.getColumn());

			if (isSuppressed) {
				return IAdhocFilter.MATCH_ALL;
			} else {
				return filter;
			}
		} else if (filter instanceof IAndFilter andFilter) {
			List<IAdhocFilter> unfiltered = andFilter.getOperands()
					.stream()
					.map(subFilter -> suppressColumn(subFilter, suppressedColumns))
					.toList();

			return AndFilter.and(unfiltered);
		} else if (filter instanceof IOrFilter orFilter) {
			List<IAdhocFilter> unfiltered = orFilter.getOperands()
					.stream()
					.map(subFilter -> suppressColumn(subFilter, suppressedColumns))
					.toList();

			return OrFilter.or(unfiltered);
		} else {
			throw new NotYetImplementedException("filter:%s".formatted(filter));
		}
	}

	/**
	 * 
	 * @param suppressedColumns
	 * @return an {@link IFilterEditor} which turns suppressedColumns into `matchAll`
	 */
	public static IFilterEditor suppressColumn(Set<String> suppressedColumns) {
		return filter -> suppressColumn(filter, suppressedColumns);
	}
}
