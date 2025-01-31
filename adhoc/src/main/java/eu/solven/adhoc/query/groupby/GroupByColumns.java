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

import java.util.Collection;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Lists;

import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * TODO Should this superseed {@link GroupBySimpleColumns}?
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@Slf4j
public class GroupByColumns implements IAdhocGroupBy {
	@Default
	@NonNull
	final NavigableMap<String, IAdhocColumn> nameToColumn = new TreeMap<>();

	@Override
	public String toString() {
		if (isGrandTotal()) {
			return "grandTotal";
		}

		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", nameToColumn.size());

		AtomicInteger index = new AtomicInteger();
		nameToColumn.entrySet().stream().limit(5).forEach(filter -> {
			toStringHelper.add("#" + index.getAndIncrement(), filter);
		});

		return toStringHelper.toString();
	}

	public static IAdhocGroupBy of(Collection<? extends IAdhocColumn> columns) {
		NavigableMap<String, IAdhocColumn> nameToColumn = new TreeMap<>();

		columns.forEach(column -> {
			String columnName = column.getColumn();
			if (nameToColumn.containsKey(columnName)) {
				if (nameToColumn.get(columnName).equals(column)) {
					log.trace("Column={} is expressed multiple times", column);
				} else {
					throw new IllegalArgumentException("Multiple columns with same name: %s and %s");
				}
			}
			nameToColumn.put(columnName, column);
		});

		return GroupByColumns.builder().nameToColumn(nameToColumn).build();
	}

	public static IAdhocGroupBy of(IAdhocColumn wildcard, IAdhocColumn... wildcards) {
		return of(Lists.asList(wildcard, wildcards));
	}

	public static IAdhocGroupBy named(Collection<String> columns) {
		return of(columns.stream().map(c -> ReferencedColumn.ref(c)).toList());
	}

	public static IAdhocGroupBy named(String column, String... moreColumns) {
		return named(Lists.asList(column, moreColumns));
	}

	public static class GroupByColumnsBuilder {
		/**
		 * 
		 * @param column
		 *            ad additional column to groupBy
		 * @return the mutated builder
		 */
		public GroupByColumnsBuilder column(IAdhocColumn column) {
			NavigableMap<String, IAdhocColumn> currentColumns = this.build().getNameToColumn();

			NavigableMap<String, IAdhocColumn> newColumns = new TreeMap<>(currentColumns);
			newColumns.put(column.getColumn(), column);

			this.nameToColumn(newColumns);

			return this;
		}
	}

	/**
	 * 
	 * @return grandTotal as there is no wildcard column.
	 */
	public static IAdhocGroupBy grandTotal() {
		return GroupByColumns.named(Set.of());
	}

}
