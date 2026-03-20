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
package eu.solven.adhoc.cuboid.slice;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.BiConsumer;

import eu.solven.adhoc.cuboid.tabular.ITabularGroupBySlice;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.IHasAdhocMap;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.query.cube.IGroupBy;

/**
 * A slice expresses the coordinates of an output row, given columns expressed by a {@link IGroupBy}.
 * 
 * Coordinates (e.g. Map values) are normalized: e.g. `int` and `long` are considered equals.
 * 
 * Similar to {@link IAdhocMap}, but slightly more specific to a `GROUP BY` entry representation.
 * 
 * In term of wording, this may be referred as a cell. However, a cell is the finest data-block in a spreadsheet, which
 * relates with Adhoc as it brings spreadsheet-like Formulas. An Adhoc slice is then a Excel Pivot-table cell .
 * 
 * @author Benoit Lacelle
 */
public interface ISlice extends Comparable<ISlice>, ITabularGroupBySlice, IHasAdhocMap, IHasSlice {

	@Override
	default ISlice asSlice() {
		return this;
	}

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

	// BEWARE This usage is unclear, and may be a flawed design
	@Deprecated
	default Map<String, ?> getCoordinates() {
		return asAdhocMap();
	}

	@Deprecated(since = "Is this good design?")
	ISlice addColumns(Map<String, ?> masks);

	ISliceFactory getFactory();

	/**
	 * Make an immutable copy, where only given columns are retained.
	 * 
	 * @param columns
	 * @return
	 */
	ISlice retainAll(NavigableSet<String> columns);

	@Override
	default void forEachGroupBy(BiConsumer<? super String, ? super Object> action) {
		asAdhocMap().forEach(action);
	}

	@Override
	default Set<String> columnsKeySet() {
		return asAdhocMap().keySet();
	}

}
