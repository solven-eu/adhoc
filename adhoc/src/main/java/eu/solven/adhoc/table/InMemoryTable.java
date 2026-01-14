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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.ClassUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.SuppliedTabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.MoreFilterHelpers;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.spring.IHasHealthDetails;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple {@link ITableWrapper} over a {@link List} of {@link Map}. It has some specificities: it does not execute
 * groupBys, nor it handles calculated columns (over SQL expressions).
 *
 * @author Benoit Lacelle
 */
@Slf4j
@SuperBuilder
public class InMemoryTable implements ITableWrapper, IHasHealthDetails {

	@Default
	@NonNull
	@Getter
	String name = "inMemory";

	@NonNull
	@Default
	List<Map<String, ?>> rows = new ArrayList<>();

	@Default
	boolean distinctSlices = false;

	@Default
	boolean throwOnUnknownColumn = true;

	// This is useful to collect in one go all columns expected by a forest
	@Getter
	final Set<String> unknownColumns = new ConcurrentSkipListSet<>();

	public static InMemoryTable newInstance(Map<String, ?> options) {
		return InMemoryTable.builder().build();
	}

	public void add(Map<String, ?> row) {
		rows.add(row);
	}

	protected Stream<Map<String, ?>> stream() {
		return rows.stream();
	}

	@Override
	public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV2 tableQuery) {
		if (!this.equals(queryPod.getTable())) {
			throw new IllegalStateException("Inconsistent tables: %s vs %s".formatted(queryPod.getTable(), this));
		}

		Set<String> filteredColumns = FilterHelpers.getFilteredColumns(tableQuery.getFilter());
		if (tableQuery.getAggregators()
				.stream()
				.map(FilteredAggregator::getAggregator)
				// if the aggregator name is also a column name, then the filtering is valid (as we'll filter on the
				// column)
				.filter(a -> !a.getName().equals(a.getColumnName()))
				.anyMatch(a -> filteredColumns.contains(a.getName()))) {
			// This may be lifted when InMemoryTable aggregates slices instead of returning rows
			// e.g. `SELECT c, SUM(k) AS k WHERE k >= 100`
			throw new IllegalArgumentException(
					"InMemoryTable can not filter a measure. query=%s".formatted(tableQuery));
		}

		Set<String> tableColumns = getColumnTypes().keySet();
		checkKnownColumns(tableColumns, filteredColumns, "filtered");

		Set<String> aggregateFilteredColumns = tableQuery.getAggregators()
				.stream()
				.map(FilteredAggregator::getFilter)
				.flatMap(f -> FilterHelpers.getFilteredColumns(f).stream())
				.collect(ImmutableSet.toImmutableSet());
		checkKnownColumns(tableColumns, aggregateFilteredColumns, "aggregateFiltered");

		Set<String> aggregateColumns = tableQuery.getAggregators()
				.stream()
				.map(a -> a.getAggregator().getColumnName())
				.map(this::clearColumnName)
				.collect(Collectors.toSet());
		{
			Set<String> aggregateColumnsFromTable = aggregateColumns.stream()
					.filter(s -> !s.equals(Aggregator.empty().getColumnName()))
					.filter(s -> !ICountMeasuresConstants.ASTERISK.equals(s))
					.collect(ImmutableSet.toImmutableSet());

			checkKnownColumns(tableColumns, aggregateColumnsFromTable, "aggregated");
		}

		boolean isEmptyAggregation =
				tableQuery.getAggregators().isEmpty() || EmptyAggregation.isEmpty(tableQuery.getAggregators());

		Set<String> groupByColumns = getGroupByColumns(tableQuery);
		checkKnownColumns(tableColumns, groupByColumns, "groupBy");

		if (queryPod.isExplain()) {
			log.info("[EXPLAIN] tableQuery: {}", tableQuery);
		}

		return new SuppliedTabularRecordStream(tableQuery, distinctSlices, () -> {
			Stream<Map<String, ?>> matchingRows = this.stream().filter(row -> {
				return MoreFilterHelpers.match(tableQuery.getFilter(), row);
			});

			SetMultimap<String, FilteredAggregator> columnToAggregators = HashMultimap.create();
			aggregateColumns.forEach(aggregatedColumn -> {
				tableQuery.getAggregators()
						.stream()
						.filter(a -> a.getAggregator().getColumnName().equals(aggregatedColumn))
						.forEach(a -> columnToAggregators.put(aggregatedColumn, a));
			});

			ISliceFactory sliceFactory = queryPod.getSliceFactory();

			Stream<ITabularRecord> stream = matchingRows.map(row -> {
				return toRecord(sliceFactory, tableQuery, columnToAggregators, groupByColumns, row);
			});

			if (isEmptyAggregation) {
				// TODO Enable aggregations from InMemoryTable, even if there is actual aggregations

				// groupBy groupedByColumns
				Map<IAdhocSlice, Optional<ITabularRecord>> groupedAggregatedRecord =
						stream.collect(Collectors.groupingBy(ITabularRecord::getGroupBys,
								// empty is legit as we query no measure
								Collectors.reducing((left, right) -> TabularRecordOverMaps.empty())));

				return groupedAggregatedRecord.entrySet()
						.stream()
						.filter(e -> e.getValue().isPresent())
						.map(e -> TabularRecordOverMaps.builder()
								.slice(e.getKey())
								.aggregates(e.getValue().get().aggregatesAsMap())
								.build());
			} else {
				if (distinctSlices) {
					List<ITabularRecord> asList = stream.toList();

					long nbSlices = asList.stream().map(ITabularRecord::columnsKeySet).count();
					if (nbSlices != asList.size()) {
						// TODO We may implement the aggregations, but it may be unnecessary for unitTests
						throw new IllegalStateException("Rows does not enable distinct groupBys");
					}

					return asList.stream();
				} else {
					// This may publish multiple record with the same groupBy
					return stream;
				}

			}
		});
	}

	/**
	 *
	 * @param column
	 *            a column name, potentially wrapped with `"`.
	 * @return a clear columnName
	 */
	protected String clearColumnName(String column) {
		if (column.matches("\"[^\"]+\"")) {
			// columns is wrapped in double quotes, typically due to having a dot
			// e.g. `"some.column"` is not a joined column but a base column with a `.` in its name.
			return column.substring(1, column.length() - 1);
		} else {
			return column;
		}
	}

	protected Set<String> getGroupByColumns(TableQueryV2 tableQuery) {
		return tableQuery.getGroupBy()
				.getGroupedByColumns()
				.stream()
				.map(this::clearColumnName)
				.collect(Collectors.toCollection(TreeSet::new));
	}

	/**
	 *
	 * @param tableColumns
	 *            all columns known by this table, given rows
	 * @param queriedColumns
	 *            columns requested the the {@link TableQueryV2}
	 * @param columnUse
	 *            some type to be referred in logs
	 */
	protected void checkKnownColumns(Set<String> tableColumns, Set<String> queriedColumns, String columnUse) {
		Set<String> unknownQueriedColumns = Sets.difference(queriedColumns, tableColumns);
		if (!unknownQueriedColumns.isEmpty()) {
			unknownColumns.addAll(unknownQueriedColumns);

			String msg = "Unknown %s columns: %s".formatted(columnUse, unknownQueriedColumns);
			if (throwOnUnknownColumn) {
				throw new IllegalArgumentException(msg);
			} else {
				log.warn(msg);
			}
		}
	}

	protected ITabularRecord toRecord(ISliceFactory sliceFactory,
			TableQueryV2 tableQuery,
			SetMultimap<String, FilteredAggregator> columnToAggregators,
			Set<String> groupByColumns,
			Map<String, ?> row) {
		Map<String, Object> aggregates = LinkedHashMap.newLinkedHashMap(tableQuery.getAggregators().size());

		columnToAggregators.asMap().forEach((aggregatedColumn, aggs) -> {
			Object aggregatorUnderlyingValue = row.get(aggregatedColumn);
			aggs.forEach(a -> {
				if (!MoreFilterHelpers.match(a.getFilter(), row)) {
					// This aggregate is rejected by the `FILTER` clause
					return;
				}

				Object aggregate = null;
				if (CountAggregation.isCount(a.getAggregator().getAggregationKey())) {
					boolean doCountOne = false;

					if (ICountMeasuresConstants.ASTERISK.equals(aggregatedColumn)) {
						// `COUNT(*)` counts even if there is no value
						doCountOne = true;
					} else if (aggregatorUnderlyingValue != null) {
						// COUNT 1 only if the COUNTed column is not null
						doCountOne = true;
					}

					if (doCountOne) {
						aggregate = 1L;
					}
				} else if (aggregatorUnderlyingValue != null) {
					// SUM, MIN, MAX, AVG, RANK, etc

					// Transcode from columnName to aggregatorName, supposing all aggregation functions does not
					// change a not aggregated single value
					aggregate = aggregatorUnderlyingValue;
				}
				if (null != aggregate) {
					aggregates.put(a.getAlias(), aggregate);
				}
			});
		});

		IMapBuilderPreKeys groupByBuilder = sliceFactory.newMapBuilder(groupByColumns);
		groupByColumns.forEach(groupByColumn -> {
			Object value = row.get(groupByColumn);
			groupByBuilder.append(value);
		});

		return TabularRecordOverMaps.builder().aggregates(aggregates).slice(groupByBuilder.build().asSlice()).build();
	}

	@Override
	public List<ColumnMetadata> getColumns() {
		SetMultimap<String, Class<?>> columnToClasses = MultimapBuilder.hashKeys().hashSetValues().build();
		Set<String> nullableColumns = new HashSet<>();

		rows.forEach(row -> row.forEach((k, v) -> {
			if (v == null) {
				nullableColumns.add(k);
			} else {
				columnToClasses.put(k, v.getClass());
			}
		}));

		Map<String, Class<?>> columnToClass = new HashMap<>();

		columnToClasses.asMap().forEach((column, classes) -> {
			if (classes.size() == 1) {
				columnToClass.put(column, classes.iterator().next());
			} else if (classes.isEmpty()) {
				throw new IllegalStateException("No class for column=%s in %s".formatted(column, columnToClasses));
			} else {
				// Relates with eu.solven.adhoc.column.ColumnMetadata.merge(Collection<? extends ColumnMetadata>)
				Class<?> type = classes.stream().reduce(ClassUtils::determineCommonAncestor).orElse(Object.class);
				columnToClass.put(column, type);
			}
		});

		return columnToClass.entrySet()
				.stream()
				.map(e -> ColumnMetadata.builder().name(e.getKey()).type(e.getValue()).build())
				.toList();
	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", rows.size());

		AtomicInteger index = new AtomicInteger();

		stream().limit(AdhocUnsafe.getLimitOrdinalToString())
				.forEach(entry -> toStringHelper.add("#" + index.getAndIncrement(), entry));

		return toStringHelper.toString();
	}

	@Override
	public Map<String, ?> getHealthDetails() {
		return ImmutableMap.of("rows", rows.size());
	}

}
