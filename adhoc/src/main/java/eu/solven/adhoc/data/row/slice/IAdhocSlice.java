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
package eu.solven.adhoc.data.row.slice;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.data.row.ITabularGroupByRecord;
import eu.solven.adhoc.map.ISliceFactory;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;

/**
 * A slice expresses the coordinates of an output row, given columns expressed by a a {@link IAdhocGroupBy}.
 * 
 * Coordinates (e.g. Map values) are normalized: e.g. `int` and `long` are considered equals.
 * 
 * @author Benoit Lacelle
 */
public interface IAdhocSlice extends Comparable<IAdhocSlice>, ITabularGroupByRecord {
	/**
	 * 
	 * @return true if this a grandTotal slice.
	 */
	boolean isEmpty();

	/**
	 *
	 * @return an {@link ISliceFilter} equivalent to this slice. It is never `matchNone`. It is always equivalent to a
	 *         {@link AndFilter} of {@link EqualsMatcher}.
	 */
	ISliceFilter asFilter();

	/**
	 * Differs from {@link #getGroupBy(String)} as it will not fail if the column is not groupedBy.
	 *
	 * @param column
	 * @return the {@link Optional} filtered value along given column.
	 */
	Optional<Object> optGroupBy(String column);

	/**
	 * 
	 * @param columns
	 * @return a {@link Map} of available columns.
	 */
	Map<String, ?> optGroupBy(Set<String> columns);

	// BEWARE This usage is unclear, and may be a flawed design
	@Deprecated
	default Map<String, ?> getCoordinates() {
		Map<String, Object> asMap = new LinkedHashMap<>();

		columnsKeySet().forEach(column -> {
			asMap.put(column, getGroupBy(column, Object.class));
		});

		return asMap;
	}

	@Deprecated(since = "Is this good design?")
	IAdhocSlice addColumns(Map<String, ?> masks);

	ISliceFactory getFactory();

}
