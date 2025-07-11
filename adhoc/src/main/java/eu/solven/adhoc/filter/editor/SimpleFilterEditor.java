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
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
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
	ImmutableMap<String, ?> columnToValues;

	@Override
	public IAdhocFilter editFilter(IAdhocFilter input) {
		AtomicReference<IAdhocFilter> edited = new AtomicReference<>(input);

		columnToValues.forEach((column, value) -> edited.set(shiftIfPresent(edited.get(), column, value)));

		return edited.get();
	}

	@SuppressWarnings("PMD.FieldNamingConventions")
	private enum FilterMode {
		// Impact column filter on the considered column, else do nothing.
		// Accept a `Function`, as there is always a value to shift
		shiftIfPresent,
		// This is equivalent to `AND(suppressColumn(filter),column==value)`
		alwaysShift,

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
		return shift(filter, column, value, FilterMode.alwaysShift);
	}

	/**
	 *
	 * @param filter
	 *            the original filter
	 * @param column
	 *            the column to filter. If it is not already expressed, the filter is not shifted.
	 * @param value
	 *            if value is a {@link Function}, it is applied to IValueMatcher operands.
	 * @return like {@link #shift(String, Object, IAdhocFilter)} but only if the column is expressed
	 */
	public static IAdhocFilter shiftIfPresent(IAdhocFilter filter, String column, Object value) {
		return shift(filter, column, value, FilterMode.shiftIfPresent);
	}

	protected static IAdhocFilter shift(IAdhocFilter filter, String column, Object value, FilterMode filterMode) {
		if (filter.isMatchNone()) {
			return filter;
		} else if (filter.isMatchAll()) {
			if (filterMode == FilterMode.alwaysShift) {
				return ColumnFilter.isEqualTo(column, value);
			} else {
				return filter;
			}
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			IAdhocFilter shiftColumn = toFilter(columnFilter, column, value);
			if (columnFilter.getColumn().equals(column)) {
				// Replace the valueMatcher by the shift
				return shiftColumn;
			} else if (filterMode == FilterMode.alwaysShift) {
				// Combine both columns
				return AndFilter.and(columnFilter, shiftColumn);
			} else {
				return filter;
			}
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			Set<IAdhocFilter> operands = andFilter.getOperands();

			List<IAdhocFilter> shiftedOperands =
					operands.stream().map(f -> shift(f, column, value, filterMode)).toList();

			return AndFilter.and(shiftedOperands);
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			Set<IAdhocFilter> operands = orFilter.getOperands();

			List<IAdhocFilter> shiftedOperands =
					operands.stream().map(f -> shift(f, column, value, filterMode)).toList();

			return OrFilter.or(shiftedOperands);
		} else {
			throw new NotYetImplementedException("filter=%s".formatted(filter));
		}
	}

	private static IAdhocFilter toFilter(IColumnFilter columnFilter, String column, Object value) {
		if (value instanceof Function valueShifter) {
			IValueMatcher shiftedValueMatcher = ShiftedValueMatcher.shift(columnFilter.getValueMatcher(), valueShifter);
			return ColumnFilter.builder().column(column).valueMatcher(shiftedValueMatcher).build();
		} else {
			// Replace the valueMatcher by the shift
			return ColumnFilter.isEqualTo(column, value);
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
