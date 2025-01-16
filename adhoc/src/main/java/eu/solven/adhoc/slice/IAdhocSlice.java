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
package eu.solven.adhoc.slice;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.pepper.core.PepperLogHelper;

/**
 * A slice expresses the axes along which a query is filtered.
 */
public interface IAdhocSlice {
	/**
	 * The columns for which a filter is expressed
	 */
	Set<String> getColumns();

	/**
	 *
	 * @param column
	 * @return the filtered coordinate, only if the column is actually filtered. It may be a {@link Collection} if the
	 *         column is filtered along multiple values.
	 */
	default Object getRawFilter(String column) {
		return optFilter(column)
				.orElseThrow(() -> new IllegalArgumentException("%s is not a sliced column".formatted(column)));
	}

	/**
	 *
	 * @param column
	 * @param clazz
	 * @return the filtered coordinate on given column. Unless clazz accept {@link java.util.Collection}, this would
	 *         match only on simple (single value) filters.
	 * @param <T>
	 */
	default <T> T getFilter(String column, Class<? extends T> clazz) {
		Object filter = getRawFilter(column);

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
	Optional<Object> optFilter(String column);

	Map<String, ?> optFilters(Set<String> columns);

	// BEWARE This usage is unclear, and may be a flawed design
	@Deprecated
	default Map<String, Object> getCoordinates() {
		Map<String, Object> asMap = new LinkedHashMap<>();

		getColumns().forEach(column -> {
			asMap.put(column, getFilter(column, Object.class));
		});

		return asMap;
	}

	AdhocSliceAsMap getAdhocSliceAsMap();
}
