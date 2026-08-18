/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.export.excel;

import java.util.List;
import java.util.Set;

/**
 * Per-row context supplied to {@link IMeasureExcelFormula#translate}: cell references the translator needs to assemble
 * its formula. Exposes the underlying-measure cell references (positional, same order as
 * {@link eu.solven.adhoc.model.measure.Combinator#getUnderlyings()}) and, for filter-aware translators like
 * {@code FiltratorFormula}, the cell references to the groupBy columns at the current row.
 *
 * @author Benoit Lacelle
 */
public interface RowContext {

	/** @return positional cell references for each of the measure's underlyings, e.g. {@code ["B2", "C2"]}. */
	List<String> getUnderlyingCellRefs();

	/**
	 * @param columnName
	 *            a groupBy column name (must be in {@link #getGroupByColumns()})
	 * @return the Excel cell reference for that groupBy column's value at the current row, e.g. {@code "A2"}.
	 * @throws IllegalArgumentException
	 *             if {@code columnName} is not part of the current query's groupBy.
	 */
	String getGroupByCellRef(String columnName);

	/** @return the names of groupBy columns visible at the current row. */
	Set<String> getGroupByColumns();
}
