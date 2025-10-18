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
package eu.solven.adhoc.data.row;

import java.util.Collection;
import java.util.Set;
import java.util.function.BiConsumer;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.IHasColumnsKeySet;
import eu.solven.pepper.core.PepperLogHelper;

/**
 * Used to hold the slice (given the groupBy) from {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 */
public interface ITabularGroupByRecord extends IHasColumnsKeySet {

	IAdhocSlice getGroupBys();

	/**
	 * The columns for which a coordinate is expressed
	 */
	@Override
	Set<String> columnsKeySet();

	/**
	 *
	 * @param column
	 * @return the sliced coordinate, only if the column is actually sliced. Can not be a {@link Collection} nor a
	 *         {@link eu.solven.adhoc.query.filter.value.IValueMatcher}. May be null.
	 */
	@Nullable
	Object getGroupBy(String column);

	/**
	 *
	 * @param column
	 * @param clazz
	 * @return the filtered coordinate on given column. Can not be a {@link Collection} nor a
	 *         {@link eu.solven.adhoc.query.filter.value.IValueMatcher}.
	 * @param <T>
	 */
	default <T> T getGroupBy(String column, Class<? extends T> clazz) {
		Object filter = getGroupBy(column);

		if (clazz.isInstance(filter)) {
			return clazz.cast(filter);
		} else {
			throw new IllegalArgumentException("column=%s is missing or with unexpected type: %s (expected class=%s)"
					.formatted(column, PepperLogHelper.getObjectAndClass(filter), clazz));
		}
	}

	void forEachGroupBy(BiConsumer<? super String, ? super Object> action);
}
