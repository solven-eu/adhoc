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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.dag.TableAggregatesMetadata;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.record.IAggregatedRecordStream;
import eu.solven.adhoc.util.IHasColumns;
import eu.solven.adhoc.util.IHasName;

/**
 * Wraps a database (actually storing data for {@link IAdhocQuery}) to be queried by {@link IAdhocQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAdhocTableWrapper extends IHasColumns, IHasName {

	/**
	 *
	 * @param tableQuery
	 * @return a {@link IAggregatedRecordStream} matching the input dpQuery
	 */
	IAggregatedRecordStream streamSlices(TableQuery tableQuery);

	default CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		TableQuery tableQuery = TableQuery.builder().groupBy(GroupByColumns.named(column)).build();

		int returnedCoordinates;
		List<Object> distinctCoordinates;

		if (limit < 1) {
			returnedCoordinates = Integer.MAX_VALUE;
			distinctCoordinates = new ArrayList<>();
		} else {
			returnedCoordinates = limit;
			distinctCoordinates = new ArrayList<>(limit);
		}

		long estimatedCardinality = this.streamSlices(tableQuery)
				.asMap()
				.map(r -> r.getGroupBy(column))
				// `.disinct()` is relevant only for InMemoryTable and related tables
				.distinct()
				.peek(coordinate -> {
					if (distinctCoordinates.size() < returnedCoordinates) {
						distinctCoordinates.add(coordinate);
					}
				})
				.count();

		return CoordinatesSample.builder()
				.coordinates(distinctCoordinates)
				.estimatedCardinality(estimatedCardinality)
				.build();
	}

	/**
	 * 
	 * @param columnToAggregators
	 * @return hints about which aggregates can be executed by the table or not
	 */
	default TableAggregatesMetadata getAggregatesMetadata(SetMultimap<String, Aggregator> columnToAggregators) {
		// We consider all other tables can do all aggregations
		// BEWARE What if a table would not be able to do only a subset of aggregations?
		Map<String, Aggregator> nameToPre = new LinkedHashMap<>();
		columnToAggregators.values().forEach(a -> nameToPre.put(a.getName(), a));

		SetMultimap<String, Aggregator> nameToRaw = ImmutableSetMultimap.of();

		return TableAggregatesMetadata.builder().measureToPre(nameToPre).columnToRaw(nameToRaw).build();
	}
}
