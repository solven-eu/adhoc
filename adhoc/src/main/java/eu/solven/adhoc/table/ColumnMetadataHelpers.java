/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
import java.util.List;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;

public class ColumnMetadataHelpers {

	public static CoordinatesSample getCoordinatesMostGeneric(ITableWrapper table,
			String column,
			IValueMatcher valueMatcher,
			int limit) {
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

		long estimatedCardinality = table.streamSlices(tableQuery)
				.asMap()
				.map(r -> r.getGroupBy(column))
				// TODO Should we return the information about null-ness?
				.filter(c -> c != null)
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
}
