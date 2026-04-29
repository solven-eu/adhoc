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
package eu.solven.adhoc.engine.tabular.splitter;

import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import lombok.experimental.UtilityClass;

/**
 * Utilities around inducers.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class InducerHelpers {

	/**
	 * 
	 * @param inducerColumns
	 * @param inducerFilter
	 * @param inducedFilter
	 *            it has to be laxer than inducer (i.e. this is not checked but assumed in this method).
	 * @return an {@link Optional} {@link ISliceFilter} expressing the rows to keep from `inducer` to find all and only
	 *         rows from `induced`.
	 */
	public static Optional<ISliceFilter> makeLeftoverFilter(Collection<IAdhocColumn> inducerColumns,
			ISliceFilter inducerFilter,
			ISliceFilter inducedFilter) {
		// This assert can be quite slow
		// assert FilterHelpers.isLaxerThan(inducerFilter, inducedFilter);

		// BEWARE There is NOT two different ways to filters rows from inducer for induced:
		// Either we reject the rows which are in inducer but not expected by induced,
		// Or we keep-only the rows in inducer given additional constraints in induced.
		// Indeed, we see no actual case where we could only look at inducer columns to infer irrelevant rows, without
		// actually check the induced filter.

		// This match the row which has to be kept from inducer for induced
		ISliceFilter inducedLeftoverFilter = FilterHelpers.stripWhereFromFilter(inducerFilter, inducedFilter);

		boolean hasLeftoverFilteringColumns = inducerColumns.stream()
				.map(IAdhocColumn::getName)
				.collect(ImmutableSet.toImmutableSet())
				.containsAll(FilterHelpers.getFilteredColumns(inducedLeftoverFilter));

		if (hasLeftoverFilteringColumns) {
			// Given inducer, one can filter a subset of rows based on a filter given its rows are available
			return Optional.of(inducedLeftoverFilter);
		}

		return Optional.empty();
	}
}
