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

import java.util.Map;
import java.util.Map.Entry;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.IHasColumns;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.util.IHasName;
import eu.solven.pepper.core.PepperStreamHelper;

/**
 * Wraps a database (actually storing data for {@link ICubeQuery}) to be queried by {@link ICubeQuery}.
 * 
 * Any system able to answer a query similar to `SELECT x, SUM(x') GROUP BY y WHERE z` can be easily wrapped as a
 * ITableWrapper. A fraction of such systems may be also able to return granular rows, enabling drill-through queries.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ITableWrapper extends IHasColumns, IHasName {

	/**
	 *
	 * @param queryPod
	 * @param tableQuery
	 * @return a {@link ITabularRecordStream} matching the input dpQuery
	 */
	ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV4 tableQuery);

	default ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV3 tableQuery) {
		return streamSlices(queryPod, tableQuery.toV4());
	}

	/**
	 * Could be useful for {@link ITableWrapper} not supporting `GROUPING SET`.
	 * 
	 * @param queryPod
	 * @param tableQuery
	 * @return
	 */
	default ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV2 tableQuery) {
		return streamSlices(queryPod, tableQuery.toV3());
	}

	default ITabularRecordStream streamSlices(QueryPod queryPod, TableQuery tableQuery) {
		return streamSlices(queryPod, tableQuery.toV2());
	}

	@Deprecated(since = "Used for tests, or edge-cases")
	default ITabularRecordStream streamSlices(TableQueryV3 tableQuery) {
		return streamSlices(QueryPod.forTable(this), tableQuery);
	}

	@Deprecated(since = "Used for tests, or edge-cases")
	default ITabularRecordStream streamSlices(TableQueryV2 tableQuery) {
		return streamSlices(QueryPod.forTable(this), tableQuery);
	}

	@Deprecated(since = "Used for tests, or edge-cases")
	default ITabularRecordStream streamSlices(TableQuery tableQuery) {
		return streamSlices(QueryPod.forTable(this), tableQuery);
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
				.collect(PepperStreamHelper.toLinkedMap(Entry::getKey,
						e -> getCoordinates(e.getKey(), e.getValue(), limit)));
	}
}
