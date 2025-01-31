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
package eu.solven.adhoc.query;

import java.util.List;

import eu.solven.adhoc.query.groupby.IAdhocColumn;
import eu.solven.adhoc.query.table.TableQuery;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Singular;
import lombok.Value;

/**
 * Used by {@link TableQuery} to restrict ourselves to the top results.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class AdhocTopClause {

	public static final AdhocTopClause NO_LIMIT = AdhocTopClause.builder().build();

	// If negative, do not apply any limit/top
	// https://stackoverflow.com/questions/5668540/difference-between-top-and-limit-keyword-in-sql
	private static final int NO_TOP = -1;

	@Singular
	final List<IAdhocColumn> columns;

	@Default
	final int limit = NO_TOP;
	@Default
	final boolean desc = true;

	public boolean isPresent() {
		return !columns.isEmpty();
	}
}
