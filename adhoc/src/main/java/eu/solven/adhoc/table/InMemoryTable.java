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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.adhoc.measure.step.Aggregator;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.record.AggregatedRecordOverMaps;
import eu.solven.adhoc.record.IAggregatedRecordStream;
import eu.solven.adhoc.record.SuppliedAggregatedRecordStream;
import eu.solven.adhoc.table.transcoder.IdentityImplicitTranscoder;
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
	public IAggregatedRecordStream streamSlices(TableQuery tableQuery) {
		Set<String> aggregateColumns =
				tableQuery.getAggregators().stream().map(Aggregator::getColumnName).collect(Collectors.toSet());
		Set<String> groupByColumns = new HashSet<>(tableQuery.getGroupBy().getGroupedByColumns());

		return new SuppliedAggregatedRecordStream(tableQuery, () -> this.stream().filter(row -> {
			return FilterHelpers.match(new IdentityImplicitTranscoder(), tableQuery.getFilter(), row);
		}).map(row -> {
			Map<String, Object> aggregates = new LinkedHashMap<>();
			// Transcode from columnName to aggregatorName, supposing all aggregation functions does not change a not
			// aggregated single value
			aggregateColumns.forEach(aggregatedColumn -> {
				Object aggregatorUnderlyingValue = row.get(aggregatedColumn);
				if (aggregatorUnderlyingValue != null) {
					tableQuery.getAggregators()
							.stream()
							.filter(a -> a.getColumnName().equals(aggregatedColumn))
							.forEach(a -> aggregates.put(a.getName(), aggregatorUnderlyingValue));
				}
			});

			Map<String, Object> groupBys = row.entrySet()
					.stream()
					.filter(e -> groupByColumns.contains(e.getKey()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

			return AggregatedRecordOverMaps.builder().aggregates(aggregates).groupBys(groupBys).build();
		}));
	}

	@Override
	public Map<String, Class<?>> getColumns() {
		return rows.stream().flatMap(row -> row.keySet().stream()).collect(Collectors.toMap(c -> c, c -> Object.class));
	}

}
