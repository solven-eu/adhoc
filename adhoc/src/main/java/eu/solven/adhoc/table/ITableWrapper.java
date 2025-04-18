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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.dag.context.ExecutingQueryContext;
import eu.solven.adhoc.dag.tabular.TableAggregatesMetadata;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.util.IHasColumns;
import eu.solven.adhoc.util.IHasName;

/**
 * Wraps a database (actually storing data for {@link IAdhocQuery}) to be queried by {@link IAdhocQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ITableWrapper extends IHasColumns, IHasName {

	/**
	 *
	 * @param executingQueryContext
	 * @param tableQuery
	 * @return a {@link ITabularRecordStream} matching the input dpQuery
	 */
	ITabularRecordStream streamSlices(ExecutingQueryContext executingQueryContext, TableQuery tableQuery);

	@Deprecated(since = "Used for tests, or edge-cases")
	default ITabularRecordStream streamSlices(TableQuery tableQuery) {
		return streamSlices(ExecutingQueryContext.forTable(this), tableQuery);
	}

	/**
	 * 
	 * @param column
	 * @param valueMatcher
	 * @param limit
	 *            the maximum number of sample coordinates to return
	 * @return
	 */
	default CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		return ColumnMetadataHelpers.getCoordinatesMostGeneric(this, column, valueMatcher, limit);
	}

	/**
	 * 
	 * @param columnToValueMatcher
	 * @param limit
	 *            the maximum number of sample coordinates to return by column
	 * @return
	 */
	default Map<String, CoordinatesSample> getCoordinates(Map<String, IValueMatcher> columnToValueMatcher, int limit) {
		return columnToValueMatcher.entrySet()
				.stream()
				.collect(Collectors.toMap(e -> e.getKey(), e -> getCoordinates(e.getKey(), e.getValue(), limit)));
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
