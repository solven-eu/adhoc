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
package eu.solven.adhoc.engine.step;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * A simple {@link IAdhocSlice} based on a {@link Map}
 * 
 * @author Benoit Lacelle
 */
@Builder
@ToString
public class SliceReader implements ISliceReader {
	// filter expressed by the groupBy (as each slice materialize a coordinate for each groupedBy column)
	@NonNull
	@Getter
	final ISliceFilter sliceFilter;

	// filter expressed by the queryStep own filter
	@NonNull
	@Getter
	final ISliceFilter stepFilter;

	// This cache is relevant as some transformator may request the filter multiple times, to extract multiple columns
	final Supplier<ISliceFilter> filterSupplier = Suppliers.memoize(this::asFilterNoCache);

	@Override
	public ISliceFilter asFilter() {
		return filterSupplier.get();
	}

	public ISliceFilter asFilterNoCache() {
		// AND the slice with the step as the step may express some filters which are not in the slice
		// e.g. if we filter color=red and groupBy country: slice would express only country=FR
		ISliceFilter filter = AndFilter.and(sliceFilter, stepFilter);

		if (filter.isMatchNone()) {
			// These cases are unclear.
			// One occurrence was due to improper type conversion
			// e.g. a filter with wrong type `y=2025` as String, while receiving a slice with `y=2025` as long.
			// Another occurrence is Dispatchor writing into Slice which are filtered out. May happen on simple but
			// inefficient IDecomposition writing in filtered slices.
			throw new IllegalStateException("AND between slice=`%s` and query.filter=`%s` led to .matchNone"
					.formatted(sliceFilter, stepFilter));
		}

		// BEWARE We should also check it is always an `AND` of `EQUALS`.
		return filter;
	}

	@Override
	public IValueMatcher getValueMatcher(String column) {
		// Priority to the sliceFilter, as it is equivalent to a Map of single value operands
		IValueMatcher sliceValueMatcher = FilterHelpers.getValueMatcher(sliceFilter, column);
		if (!IValueMatcher.MATCH_ALL.equals(sliceValueMatcher)) {
			// If there is an actual filter, it should be compatible with the stepFilter: no need to check again for it
			return sliceValueMatcher;
		}

		IValueMatcher stepValueMatcher = FilterHelpers.getValueMatcher(stepFilter, column);
		if (!IValueMatcher.MATCH_ALL.equals(stepValueMatcher)) {
			return stepValueMatcher;
		}

		return IValueMatcher.MATCH_ALL;
	}

	@Override
	public IValueMatcher getValueMatcherLax(String column) {
		// Priority to the sliceFilter, as it is equivalent to a Map of single value operands
		IValueMatcher sliceValueMatcher = FilterHelpers.getValueMatcherLax(sliceFilter, column);
		if (!IValueMatcher.MATCH_ALL.equals(sliceValueMatcher)) {
			// If there is an actual filter, it should be compatible with the stepFilter: no need to check again for it
			return sliceValueMatcher;
		}

		IValueMatcher stepValueMatcher = FilterHelpers.getValueMatcherLax(stepFilter, column);
		if (!IValueMatcher.MATCH_ALL.equals(stepValueMatcher)) {
			return stepValueMatcher;
		}

		return IValueMatcher.MATCH_ALL;
	}

	@Override
	public Set<String> getFilteredColumns() {
		List<String> filteredColumns = new ArrayList<>();

		// filtered columns is the union of filters by slice and by step
		filteredColumns.addAll(FilterHelpers.getFilteredColumns(sliceFilter));
		filteredColumns.addAll(FilterHelpers.getFilteredColumns(stepFilter));

		return ImmutableSet.copyOf(filteredColumns);
	}

}
