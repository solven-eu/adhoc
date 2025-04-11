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
package eu.solven.adhoc.query.filter;

import java.util.Collection;

import javax.annotation.Nonnull;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;

/**
 * Filter along a specific column. Typically, for `=` or `IN` matchers.
 *
 * @author Benoit Lacelle
 */
public interface IColumnFilter extends IAdhocFilter {

	/**
	 * @return the name of the filtered column.
	 */
	@Nonnull
	String getColumn();

	/**
	 * The default is generally true, as it follows the fact that `Map.get(unknownKey)` returns null.
	 *
	 * BEWARE Explain actual use-case for this. May it plays a role for CompositeCube, when a column is missing on one subCube?
	 *
	 * @return true if a missing column would behave like containing NULL.
	 */
	boolean isNullIfAbsent();

	/**
	 * The filter could be null, a {@link Collection} for a `IN` clause, a {@link LikeMatcher}, else it is interpreted
	 * as an `=` clause.
	 *
	 * @return the filtered value.
	 */
	IValueMatcher getValueMatcher();

}