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

import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.adhoc.query.cube.IAdhocQuery;
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

	default Set<Object> getCoordinates(String column) {
		TableQuery tableQuery = TableQuery.builder().groupBy(GroupByColumns.named(column)).build();
		return this.streamSlices(tableQuery).asMap().map(r -> r.getGroupBy(column)).collect(Collectors.toSet());
	}

}
