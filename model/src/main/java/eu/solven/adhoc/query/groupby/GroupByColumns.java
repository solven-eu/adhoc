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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.util.IHasName;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IGroupBy} based on a {@link Set} of {@link IAdhocColumn}.
 *
 * It has similar semantic with ImmutableSet (e.g. SequencedSet) even if some methods returned Ordered information.
 *
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@Slf4j
@EqualsAndHashCode(exclude = { "cachedNameToColumn", "retainedToGroupBy" })
public class GroupByColumns implements IGroupBy {

	@Singular
	@NonNull
	@Getter
	@JsonProperty
	final ImmutableSet<IAdhocColumn> columns;

	// Cached to spare memory allocation as this may be read once per aggregate
	@JsonIgnore
	final Supplier<NavigableMap<String, IAdhocColumn>> cachedNameToColumn =
			Suppliers.memoize(() -> namedColumns(getColumns()));

	// Used a cache as this may be called once per ITabularRecord
	@JsonIgnore
	final Map<Set<String>, IGroupBy> retainedToGroupBy = new ConcurrentHashMap<>();

	@Override
	public String toString() {
		if (isGrandTotal()) {
			return "grandTotal";
		}

		// However, this is similar to SequencedSet: `.toString` can be different for equals instances
		ImmutableSet<IAdhocColumn> columnForToString = getColumns();

		if (columnForToString.stream().allMatch(c -> c instanceof ReferencedColumn)) {
			return columnForToString.stream()
					.map(c -> ((ReferencedColumn) c).getName())
					.map(this::escape)
					.collect(Collectors.joining(", ", "(", ")"));
		}

		return columnForToString.stream().map(filter -> {
			if (filter instanceof ReferencedColumn ref) {
				return ref.getName();
			} else {
				return filter.toString();
			}
		}).collect(Collectors.joining(", ", "(", ")"));
	}

	@Override
	public NavigableMap<String, IAdhocColumn> getSortedNameToColumn() {
		return cachedNameToColumn.get();
	}

	/**
	 *
	 * @return grandTotal as there is no wildcard column.
	 */
	public static IGroupBy grandTotal() {
		return named(ImmutableSet.of());
	}

	public static IGroupBy named(Collection<String> columns) {
		return of(columns.stream().map(ReferencedColumn::ref).toList());
	}

	public static IGroupBy named(String column, String... moreColumns) {
		return named(Lists.asList(column, moreColumns));
	}

	public static IGroupBy of(Collection<? extends IAdhocColumn> columns) {
		// `namedColumns` is useful to check the consistency of names
		namedColumns(columns);

		return GroupByColumns.builder().columns(columns).build();
	}

	public static IGroupBy of(IAdhocColumn wildcard, IAdhocColumn... wildcards) {
		return of(Lists.asList(wildcard, wildcards));
	}

	public static <T extends IHasName> NavigableMap<String, T> namedColumns(Collection<? extends T> columns) {
		ImmutableSortedMap.Builder<String, T> nameToColumnBuilder = ImmutableSortedMap.<String, T>naturalOrder();

		Set<T> distinct = new LinkedHashSet<>();

		columns.forEach(column -> {
			if (distinct.add(column)) {
				nameToColumnBuilder.put(column.getName(), column);
			} else {
				// Typically when referencing the same column multiple times
				// Occurs when different calculatedColumns refers to the same underlying
				// TODO Should we throw if the multiple columns are not equals?
				log.trace("Skip {} as it is already in the groupBy", column);
			}
		});

		return nameToColumnBuilder.build();
	}

	protected String escape(String name) {
		// TODO There should be a utility method somewhere, as the logic is much more complex
		// e.g. escaping `,`, escaping `"`, etc.
		if (name.contains(",") && !name.matches("\".*\"")) {
			return "\"" + name + "\"";
		}
		return name;
	}

	@Override
	public @NonNull IGroupBy retainAll(Set<String> columns) {
		if (this.columns.isEmpty() || columns.isEmpty()) {
			return GRAND_TOTAL;
		}

		return retainedToGroupBy.computeIfAbsent(columns, ks -> {
			List<IAdhocColumn> retainedColumns = getColumns().stream().filter(c -> ks.contains(c.getName())).toList();

			if (retainedColumns.size() == this.columns.size()) {
				return this;
			}

			return of(retainedColumns);
		});
	}

	public static IGroupBy mergeNonAmbiguous(Set<IGroupBy> groupBys) {
		SetMultimap<String, IAdhocColumn> nameToColumns =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();
		groupBys.forEach(gb -> {
			gb.getColumns().forEach(c -> nameToColumns.put(c.getName(), c));
		});

		Optional<Map.Entry<String, Collection<IAdhocColumn>>> optFaultyEntry =
				nameToColumns.asMap().entrySet().stream().filter(e -> e.getValue().size() >= 2).findAny();
		optFaultyEntry.ifPresent(faultyEntry -> {
			throw new IllegalArgumentException(
					"Ambiguous column=%s to %s".formatted(faultyEntry.getKey(), faultyEntry.getValue()));
		});

		return of(nameToColumns.values());
	}
}
