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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.pepper.core.PepperLogHelper;

/**
 * A slice expresses the axes along which a query is filtered.
 * 
 * @author Benoit Lacelle
 */
public interface IAdhocSlice {
	/**
	 * The columns for which a filter is expressed
	 */
	Set<String> getColumns();

	/**
	 *
	 * @return an {@link IAdhocFilter} equivalent to this slice. It is never `matchNone`. It is also a {@link AndFilter}
	 *         of {@link EqualsMatcher}.
	 */
	IAdhocFilter asFilter();

	/**
	 *
	 * @param column
	 * @return the sliced coordinate, only if the column is actually sliced. Can not be a {@link Collection} nor a
	 *         {@link eu.solven.adhoc.query.filter.value.IValueMatcher}.
	 */
	default Object getRawSliced(String column) {
		return optSliced(column).orElseThrow(() -> new IllegalArgumentException(
				"%s is not a sliced column amongst %s".formatted(column, getColumns())));
	}

	/**
	 *
	 * @param column
	 * @param clazz
	 * @return the filtered coordinate on given column. Can not be a {@link Collection} nor a
	 *         {@link eu.solven.adhoc.query.filter.value.IValueMatcher}.
	 * @param <T>
	 */
	default <T> T getSliced(String column, Class<? extends T> clazz) {
		Object filter = getRawSliced(column);

		if (clazz.isInstance(filter)) {
			return clazz.cast(filter);
		} else {
			throw new IllegalArgumentException("column=%s is missing or with unexpected type: %s (expected class=%s)"
					.formatted(column, PepperLogHelper.getObjectAndClass(filter), clazz));
		}
	}

	/**
	 *
	 * @param column
	 * @return the {@link Optional} filtered value along given column.
	 */
	Optional<Object> optSliced(String column);

	Map<String, ?> optSliced(Set<String> columns);

	// BEWARE This usage is unclear, and may be a flawed design
	@Deprecated
	default Map<String, Object> getCoordinates() {
		Map<String, Object> asMap = new LinkedHashMap<>();

		getColumns().forEach(column -> {
			asMap.put(column, getSliced(column, Object.class));
		});

		return asMap;
	}

	/**
	 * 
	 * @return the simple (i.e. without the queryStep) slice, as a {@link SliceAsMap}
	 */
	SliceAsMap getAdhocSliceAsMap();

}
