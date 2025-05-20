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
package eu.solven.adhoc.table;

import java.util.*;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import eu.solven.adhoc.query.table.TableQueryV2;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Like {@link InMemoryTable}, but helping to generate dataset given the requested columns.
 */
@Slf4j
@SuperBuilder
public class InMemoryTable_Recording extends InMemoryTable {
	// InMemoryTable would throw on unknown columns: when recording, we do not want to throw
	@Override
	protected Set<String> getGroupByColumns(TableQueryV2 tableQuery, Set<String> filteredColumns) {
		Set<String> groupByColumns = new HashSet<>(tableQuery.getGroupBy().getGroupedByColumns());

		{
			Set<String> tableColumns = getColumnTypes().keySet();
			SetView<String> unknownFilteredColumns = Sets.difference(filteredColumns, tableColumns);
			if (!unknownFilteredColumns.isEmpty()) {
				log.warn("Unknown filtered columns: %s".formatted(unknownFilteredColumns));
			}

			SetView<String> unknownGroupedByColumns = Sets.difference(groupByColumns, tableColumns);
			if (!unknownGroupedByColumns.isEmpty()) {
				log.warn("Unknown groupedBy columns: %s".formatted(unknownGroupedByColumns));
			}
		}
		return groupByColumns;
	}

}
