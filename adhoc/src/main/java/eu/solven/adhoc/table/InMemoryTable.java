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

import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.table.TableQuery;
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
	public IRowsStream streamSlices(TableQuery tableQuery) {
		Set<String> queriedColumns = new HashSet<>();
		queriedColumns.addAll(tableQuery.getGroupBy().getGroupedByColumns());
		tableQuery.getAggregators().stream().map(a -> a.getColumnName()).forEach(queriedColumns::add);

		return new SuppliedRowsStream(tableQuery, () -> this.stream().filter(row -> {
			return FilterHelpers.match(new IdentityImplicitTranscoder(), tableQuery.getFilter(), row);
		}).map(row -> {
			Map<String, Object> withSelectedColumns = new LinkedHashMap<>(row);

			// Discard the column not expressed by the dbQuery
			withSelectedColumns.keySet().retainAll(queriedColumns);

			tableQuery.getAggregators().forEach(aggregator -> {
				String columnName = aggregator.getColumnName();
				Object aggregatorValue = row.get(columnName);

				// TODO This may lead to issues if we alias an aggregator with a name also used as groupBy
				withSelectedColumns.put(aggregator.getName(), aggregatorValue);
			});

			return withSelectedColumns;
		}));
	}

	@Override
	public Map<String, Class<?>> getColumns() {
		return rows.stream().flatMap(row -> row.keySet().stream()).collect(Collectors.toMap(c -> c, c -> Object.class));
	}

}
