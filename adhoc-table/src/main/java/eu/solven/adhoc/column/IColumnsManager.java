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
package eu.solven.adhoc.column;

import java.util.List;
import java.util.Set;

import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.ITableQueryPod;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManager;
import eu.solven.adhoc.util.IHasName;

/**
 * Helps managing various edge-cases around columns, like missing columns or type transcoding.
 * 
 * @author Benoit Lacelle
 * @see IMissingColumnManager
 * @see ICustomTypeManager
 * @see ITableAliaser
 */
public interface IColumnsManager extends IHasColumnTypes {

	ITabularRecordStream openSlicesStream(ITableQueryPod queryPod, TableQueryV4 tableQuery);

	/**
	 * Same transcoding/post-filter behaviour as {@link #openSlicesStream(QueryPod, TableQueryV4)}, but routes the
	 * underlying call to {@link eu.solven.adhoc.table.ITableWrapper#streamRows} so each DB row produces one
	 * {@link eu.solven.adhoc.dataframe.row.ITabularRecord}, with no GROUP BY or aggregation. Used by the
	 * {@link eu.solven.adhoc.options.StandardQueryOptions#DRILLTHROUGH} path. Takes a {@link TableQueryV3} because the
	 * DRILLTHROUGH execution does not benefit from V4's per-step (groupBy × aggregators) partitioning.
	 */
	default ITabularRecordStream openRowsStream(ITableQueryPod queryPod, TableQueryV3 tableQuery) {
		return openSlicesStream(queryPod, tableQuery.toV4());
	}

	Object onMissingColumn(IHasName cube, String column);

	Object onMissingColumn(String column);

	/**
	 * This is typically important when the table has JOINs, as a columnName may be ambiguous through the JOINs.
	 */
	AliasingContext openTranscodingContext();

	/**
	 * 
	 * @param operatorFactory
	 * @param measures
	 *            a {@link Set} of measures providing some {@link IColumnGenerator}.
	 * @param columnMatcher
	 *            filter the columnName.
	 * @return
	 */
	List<IColumnGenerator> getGeneratedColumns(IOperatorFactory operatorFactory,
			Set<IMeasure> measures,
			IValueMatcher columnMatcher);

	Set<String> getColumnAliases();

}
