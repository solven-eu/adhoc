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
package eu.solven.adhoc.query.cube;

import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * A {@link List} of columns. Typically used by {@link ICubeQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
@JsonDeserialize(as = GroupByColumns.class)
@FunctionalInterface
public interface IAdhocGroupBy {
	IAdhocGroupBy GRAND_TOTAL = GroupByColumns.grandTotal();

	/**
	 * If true, there is not a single groupBy
	 * 
	 * @return
	 */
	@JsonIgnore
	default boolean isGrandTotal() {
		return getGroupedByColumns().isEmpty();
	}

	/**
	 * Some of these columns may not be actual underlying columns, but computed given some logic. Consider using
	 * {@link #getNameToColumn()}.
	 * 
	 * @return the name of the groupBy when the input and output columns are identical.
	 */
	@JsonIgnore
	default NavigableSet<String> getGroupedByColumns() {
		return getNameToColumn().navigableKeySet();
	}

	/**
	 * 
	 * @return the mapping from the groupedBy column to the definition of given column.
	 */
	// @JsonIgnore as only the columns would be serialized. (name->column) is syntactic sugar.
	@JsonIgnore
	NavigableMap<String, IAdhocColumn> getNameToColumn();
}