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
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManager;
import eu.solven.adhoc.util.IHasColumnTypes;

/**
 * Helps managing various edge-cases around columns, like missing columns or type transcoding.
 * 
 * @author Benoit Lacelle
 * @see IMissingColumnManager
 * @see ICustomTypeManager
 * @see ITableTranscoder
 */
public interface IColumnsManager extends IHasColumnTypes {

	ITabularRecordStream openTableStream(QueryPod queryPod, TableQueryV2 tableQuery);

	Object onMissingColumn(ICubeWrapper cube, String column);

	Object onMissingColumn(String column);

	/**
	 * This is typically important when the table has JOINs, as a columnName may be ambiguous through the JOINs.
	 * 
	 * @param cubeColumn
	 *            some cube column
	 * @return the equivalent table column
	 */
	String transcodeToTable(String cubeColumn);

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

}
