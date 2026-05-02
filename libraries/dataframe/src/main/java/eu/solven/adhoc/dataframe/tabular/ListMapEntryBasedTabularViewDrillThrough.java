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
package eu.solven.adhoc.dataframe.tabular;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Ints;

import eu.solven.adhoc.options.StandardQueryOptions;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

/**
 * Extends {@link ListMapEntryBasedTabularView}, but will not align input values along slices.
 *
 * It is especially useful for {@link StandardQueryOptions#DRILLTHROUGH}.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
@Jacksonized
public class ListMapEntryBasedTabularViewDrillThrough extends ListMapEntryBasedTabularView {

	/**
	 * Sentinel string emitted in the {@code values} or {@code coordinates} {@link Map} of a {@link TabularEntry} when a
	 * column from the union schema is not applicable to a given row (e.g. heterogeneous {@link TabularEntry}s sourced
	 * from sub-queries that did not all carry the same set of columns).
	 *
	 * <p>
	 * Distinct from a real {@code null} returned by the database: a real {@code null} means "the table has no value for
	 * this row/column"; {@code SKIPPED_CELL} means "this column was not part of the query that produced this row".
	 */
	public static final String SKIPPED_CELL = "$adhoc.drillthrough.skipped_cell";

	public static ListMapEntryBasedTabularViewDrillThrough withCapacity(long expectedOutputCardinality) {
		List<TabularEntry> rawArray = new ArrayList<>(Ints.checkedCast(expectedOutputCardinality));
		return builder().entries(rawArray).build();
	}

	public static ListMapEntryBasedTabularViewDrillThrough load(ITabularView from) {
		long capacity = from.size();
		ListMapEntryBasedTabularViewDrillThrough newView = withCapacity(capacity);

		return load(from, newView);
	}

	/**
	 * Append a single raw row, as produced by a DRILLTHROUGH execution. Each call produces one new
	 * {@link TabularEntry}; no slice deduplication is performed.
	 *
	 * @param coordinates
	 *            the groupBy column values for this row.
	 * @param values
	 *            the per-aggregator column values for this row.
	 */
	public void appendRow(Map<String, ?> coordinates, Map<String, ?> values) {
		entries.add(TabularEntry.builder().coordinates(coordinates).values(values).build());
	}

	/**
	 * Will write each entry in the next row, hence preventing any conflict/aggregation on slices.
	 */
	@Override
	protected int getIndexForSlice(Map<String, ?> coordinates) {
		return entries.size();
	}
}
