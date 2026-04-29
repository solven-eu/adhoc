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
package eu.solven.adhoc.case_insensitivive;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.errorprone.annotations.ThreadSafe;

/**
 * Helps managing case-insentivity.
 * 
 * @author Benoit Lacelle
 */
// https://duckdb.org/docs/stable/sql/dialect/keywords_and_identifiers
@ThreadSafe
public class CaseInsensitiveContext {
	final NavigableMap<String, String> caseToLower = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	final Map<String, String> lowerToCase = new LinkedHashMap<>();

	/**
	 * 
	 * @param column
	 *            a column with any case. It is the first encounter which will prevail for the rest (e.g. `SELECT aA,
	 *            Aa, AA from t` will register `aA` as column matching any equivalent case column).
	 * @return
	 */
	@SuppressWarnings("PMD.AvoidSynchronizedStatement")
	public void registerColumn(String column) {
		// ROOT is the most neutral locale
		String caseInsensitive = column.toLowerCase(Locale.ROOT);

		synchronized (this) {
			if (null == lowerToCase.putIfAbsent(caseInsensitive, column)) {
				caseToLower.putIfAbsent(column, caseInsensitive);
			}
		}
	}

	/**
	 * @param column
	 *            a column name with any casing
	 * @return the canonical (first-registered) casing for this column, or the input unchanged if unknown
	 */
	@SuppressWarnings("PMD.AvoidSynchronizedStatement")
	public String normalize(String column) {
		synchronized (this) {
			String lower = caseToLower.get(column);
			if (lower == null) {
				return column;
			}
			String canonical = lowerToCase.get(lower);
			if (canonical != null) {
				return canonical;
			} else {
				return column;
			}
		}
	}
}
