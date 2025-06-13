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
package eu.solven.adhoc.util;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import eu.solven.adhoc.column.ColumnMetadata;

/**
 * Helps describing the column of some data-structure.
 * 
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IHasColumns extends IHasColumnTypes {

	/**
	 * Must be distinct per name.
	 * 
	 * @return the columns available for groupBy operations
	 */
	Collection<ColumnMetadata> getColumns();

	default Map<String, ColumnMetadata> getColumnsAsMap() {
		return getColumns().stream().collect(Collectors.toMap(c -> c.getName(), c -> c));
	}

	@Override
	default Map<String, Class<?>> getColumnTypes() {
		return getColumns().stream().collect(Collectors.toMap(ColumnMetadata::getName, ColumnMetadata::getType));
	}
}
