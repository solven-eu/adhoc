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
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.IHasName;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IAdhocGroupBy} based on a {@link Set} of {@link IAdhocColumn}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@Slf4j
@EqualsAndHashCode(exclude = "cachedNameToColumn")
public class GroupByColumns implements IAdhocGroupBy {
	// Set as not ordered
	@Singular
	@NonNull
	final ImmutableSet<IAdhocColumn> columns;

	// Cached to spare memory allocation as this may be read once per aggregate
	@JsonIgnore
	final Supplier<NavigableMap<String, IAdhocColumn>> cachedNameToColumn =
			Suppliers.memoize(() -> namedColumns(getColumns()));

	@Override
	public NavigableMap<String, IAdhocColumn> getNameToColumn() {
		return cachedNameToColumn.get();
	}

	@Override
	public String toString() {
		if (isGrandTotal()) {
			return "grandTotal";
		}

		NavigableMap<String, IAdhocColumn> nameToColumn = getNameToColumn();

		if (nameToColumn.values().stream().allMatch(c -> c instanceof ReferencedColumn)) {
			return nameToColumn.values()
					.stream()
					.map(c -> ((ReferencedColumn) c).getName())
					.collect(Collectors.joining(", ", "(", ")"));
		}

		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", nameToColumn.size());

		AtomicInteger index = new AtomicInteger();
		nameToColumn.entrySet().stream().limit(AdhocUnsafe.limitOrdinalToString).forEach(filter -> {
			toStringHelper.add("#" + index.getAndIncrement(), filter);
		});

		return toStringHelper.toString();
	}

	public static IAdhocGroupBy of(Collection<? extends IAdhocColumn> columns) {
		// `namedColumns` is useful to check the consistency of names
		namedColumns(columns);

		return GroupByColumns.builder().columns(List.copyOf(columns)).build();
	}

	public static <T extends IHasName> NavigableMap<String, T> namedColumns(Collection<? extends T> columns) {
		NavigableMap<String, T> nameToColumn = new TreeMap<>();

		columns.forEach(column -> {
			String columnName = column.getName();
			if (nameToColumn.containsKey(columnName)) {
				if (nameToColumn.get(columnName).equals(column)) {
					log.trace("Column={} is expressed multiple times in {}", column, columns);
				} else {
					throw new IllegalArgumentException("Multiple columns with same name: %s and %s");
				}
			}
			nameToColumn.put(columnName, column);
		});

		return nameToColumn;
	}

	public static IAdhocGroupBy of(IAdhocColumn wildcard, IAdhocColumn... wildcards) {
		return of(Lists.asList(wildcard, wildcards));
	}

	public static IAdhocGroupBy named(Collection<String> columns) {
		return of(columns.stream().map(ReferencedColumn::ref).toList());
	}

	public static IAdhocGroupBy named(String column, String... moreColumns) {
		return named(Lists.asList(column, moreColumns));
	}

	/**
	 * 
	 * @return grandTotal as there is no wildcard column.
	 */
	public static IAdhocGroupBy grandTotal() {
		return named(Set.of());
	}

}
