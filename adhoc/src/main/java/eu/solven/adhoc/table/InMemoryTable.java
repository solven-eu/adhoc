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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.dag.TableAggregatesMetadata;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.SuppliedTabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.table.transcoder.IdentityImplicitTranscoder;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple {@link IAdhocTableWrapper} over a {@link List} of {@link Map}. It has some specificities: it does not
 * execute groupBys, nor it handles calculated columns (over SQL expressions).
 */
@Slf4j
@Builder
public class InMemoryTable implements IAdhocTableWrapper {

	public static InMemoryTable newInstance(Map<String, ?> options) {
		return InMemoryTable.builder().build();
	}

	@Default
	@NonNull
	@Getter
	String name = "inMemory";

	@NonNull
	@Default
	List<Map<String, ?>> rows = new ArrayList<>();

	public void add(Map<String, ?> row) {
		rows.add(row);
	}

	protected Stream<Map<String, ?>> stream() {
		return rows.stream();
	}

	@Override
	public TableAggregatesMetadata getAggregatesMetadata(SetMultimap<String, Aggregator> columnToAggregators) {
		// We may receive raw columns, to be aggregated by ourselves
		ImmutableSetMultimap.Builder<String, Aggregator> nameToRawBuilder = ImmutableSetMultimap.builder();

		columnToAggregators.values().stream().forEach(a -> nameToRawBuilder.putAll(a.getColumnName(), a));
		SetMultimap<String, Aggregator> nameToRaw = nameToRawBuilder.build();

		Map<String, Aggregator> nameToPre = new LinkedHashMap<>();

		return TableAggregatesMetadata.builder().measureToPre(nameToPre).columnToRaw(nameToRaw).build();
	}

	@Override
	public ITabularRecordStream streamSlices(TableQuery tableQuery) {
		Set<String> aggregateColumns =
				tableQuery.getAggregators().stream().map(Aggregator::getColumnName).collect(Collectors.toSet());
		Set<String> groupByColumns = new HashSet<>(tableQuery.getGroupBy().getGroupedByColumns());

		int nbKeys = (int) Stream.concat(aggregateColumns.stream(), groupByColumns.stream()).distinct().count();

		return new SuppliedTabularRecordStream(tableQuery, () -> {
			Stream<Map<String, ?>> matchingRows = this.stream().filter(row -> {
				return AdhocTranscodingHelper.match(new IdentityImplicitTranscoder(), tableQuery.getFilter(), row);
			});
			Stream<ITabularRecord> stream = matchingRows.map(row -> {
				return toRecord(tableQuery, aggregateColumns, groupByColumns, nbKeys, row);
			});

			if (aggregateColumns.isEmpty() || aggregateColumns.equals(Set.of("empty"))) {
				// TODO Enable aggregations from InMemoryTable

				// groupBy groupedByColumns
				Map<Map<String, ?>, Optional<ITabularRecord>> groupedAggregatedRecord =
						stream.collect(Collectors.groupingBy(r -> r.getGroupBys(),
								// empty is legit as we query no measure
								Collectors.reducing((left, right) -> TabularRecordOverMaps.empty())));

				Stream<ITabularRecord> distinctStream = groupedAggregatedRecord.entrySet()
						.stream()
						.filter(e -> e.getValue().isPresent())
						.map(e -> TabularRecordOverMaps.builder()
								.groupBys(e.getKey())
								.aggregates(e.getValue().get().asMap())
								.build());
				return distinctStream;
			} else {
				// This may publish multiple record with the same groupBy
				return stream;
			}
		});
	}

	protected ITabularRecord toRecord(TableQuery tableQuery,
			Set<String> aggregateColumns,
			Set<String> groupByColumns,
			int nbKeys,
			Map<String, ?> row) {
		Map<String, Object> aggregates = new LinkedHashMap<>(nbKeys);
		// Transcode from columnName to aggregatorName, supposing all aggregation functions does not change a
		// not aggregated single value
		aggregateColumns.forEach(aggregatedColumn -> {
			Object aggregatorUnderlyingValue = row.get(aggregatedColumn);
			if (aggregatorUnderlyingValue != null) {
				tableQuery.getAggregators()
						.stream()
						.filter(a -> a.getColumnName().equals(aggregatedColumn))
						.forEach(a -> aggregates.put(a.getName(), aggregatorUnderlyingValue));
			} else if (ICountMeasuresConstants.ASTERISK.equals(aggregatedColumn)) {
				tableQuery.getAggregators()
						.stream()
						.filter(a -> a.getColumnName().equals(aggregatedColumn))
						// Return the column for homogeneity regarding InMemoryTable can not aggregate any
						// column
						// We could also return `a.getName()` to test a table knowing how to do a subset of
						// aggregations
						.forEach(a -> aggregates.put(a.getColumnName(), 1L));
			}
		});

		ImmutableMap.Builder<Object, Object> groupByBuilder =
				ImmutableMap.builderWithExpectedSize(groupByColumns.size());
		groupByColumns.forEach(groupByColumn -> {
			Object value = row.get(groupByColumn);
			if (value != null) {
				groupByBuilder.put(groupByColumn, value);
			}
		});

		Map<String, Object> groupBys =

				row.entrySet()
						.stream()
						.filter(e -> groupByColumns.contains(e.getKey()))
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		return TabularRecordOverMaps.builder().aggregates(aggregates).groupBys(groupBys).build();
	}

	@Override
	public Map<String, Class<?>> getColumns() {
		SetMultimap<String, Class<?>> columnToClasses = MultimapBuilder.hashKeys().hashSetValues().build();

		rows.stream().flatMap(row -> row.entrySet().stream()).forEach(e -> {
			columnToClasses.put(e.getKey(), e.getValue().getClass());
		});

		Map<String, Class<?>> columnToClass = new HashMap<>();

		columnToClasses.asMap().forEach((column, classes) -> {
			if (classes.size() == 1) {
				columnToClass.put(column, classes.iterator().next());
			} else if (classes.isEmpty()) {
				throw new IllegalStateException("No class for column=%s in %s".formatted(column, columnToClasses));
			} else {
				if (classes.stream().allMatch(Number.class::isAssignableFrom)) {
					columnToClass.put(column, Number.class);
				} else if (classes.stream().allMatch(CharSequence.class::isAssignableFrom)) {
					columnToClass.put(column, CharSequence.class);
				} else {
					columnToClass.put(column, Object.class);
				}
			}
		});

		return columnToClass;
	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", rows.size());

		AtomicInteger index = new AtomicInteger();

		stream().limit(AdhocUnsafe.limitOrdinalToString)
				.forEach(entry -> toStringHelper.add("#" + index.getAndIncrement(), entry));

		return toStringHelper.toString();
	}

}
