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
package eu.solven.adhoc.query.groupby;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.data.column.ConstantMaskMultitypeColumn;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

/**
 * Helps methods around {@link IAdhocGroupBy}
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class GroupByHelpers {

	public static IAdhocGroupBy union(IAdhocGroupBy left, @NonNull IAdhocGroupBy right) {
		Set<IAdhocColumn> union = new HashSet<>();

		union.addAll(left.getNameToColumn().values());
		union.addAll(right.getNameToColumn().values());

		if (union.isEmpty()) {
			return GroupByColumns.GRAND_TOTAL;
		} else {
			return GroupByColumns.of(union);
		}
	}

	public static IAdhocGroupBy suppressColumns(IAdhocGroupBy groupby, Set<String> suppressedColumns) {
		Map<String, IAdhocColumn> nameToColumns = new LinkedHashMap<>(groupby.getNameToColumn());

		nameToColumns.keySet().removeAll(suppressedColumns);

		return GroupByColumns.of(nameToColumns.values());
	}

	/**
	 * 
	 * @param column
	 *            the {@link IMultitypeColumnFastGet} to decorate
	 * @param mask
	 *            a mask as a {@link Map} of column to value to apply to each slice of the column
	 * @return a {@link IMultitypeColumnFastGet}
	 */
	public static IMultitypeColumnFastGet<SliceAsMap> addConstantColumns(IMultitypeColumnFastGet<SliceAsMap> column,
			Map<String, ?> mask) {
		if (mask.isEmpty()) {
			return column;
		} else {
			return ConstantMaskMultitypeColumn.builder().masked(column).mask(ImmutableMap.copyOf(mask)).build();
		}
	}

}
