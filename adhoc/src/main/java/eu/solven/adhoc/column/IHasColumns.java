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
package eu.solven.adhoc.column;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import eu.solven.adhoc.util.Blocking;
import eu.solven.adhoc.util.IHasColumnsKeySet;
import eu.solven.pepper.core.PepperStreamHelper;

/**
 * Helps describing the column of some data-structure.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IHasColumns extends IHasColumnsKeySet, IHasColumnTypes {

	/**
	 * Must be distinct per name.
	 *
	 * <p>
	 * Marked {@link Blocking} because typical implementations probe the underlying data source (JDBC metadata, remote
	 * schema fetch, sub-cube fan-out) — must not be called from {@code ForkJoinPool} workers; safe on a Virtual Thread
	 * executor.
	 *
	 * @return the columns available for groupBy operations
	 */
	@Blocking("typical implementations probe the underlying data source (JDBC metadata, remote schema, …)")
	Collection<ColumnMetadata> getColumns();

	default Map<String, ColumnMetadata> getColumnsAsMap() {
		return getColumns().stream().collect(Collectors.toMap(ColumnMetadata::getName, Function.identity(), (a, b) -> {
			throw new IllegalStateException("Duplicate key");
		}, LinkedHashMap::new));
	}

	@Override
	default Map<String, Class<?>> getColumnTypes() {
		return getColumns().stream()
				.collect(PepperStreamHelper.toLinkedMap(ColumnMetadata::getName, ColumnMetadata::getType));
	}

	@Override
	default Set<String> columnsKeySet() {
		return getColumnsAsMap().keySet();
	}
}
