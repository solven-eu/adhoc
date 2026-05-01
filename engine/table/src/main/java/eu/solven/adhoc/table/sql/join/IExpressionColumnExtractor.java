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
package eu.solven.adhoc.table.sql.join;

import java.util.Set;

/**
 * Pluggable strategy used by {@link PrunedJoinsJooqTableSupplier} to map a referenced "column" string back to the
 * underlying real columns it touches.
 * <p>
 * Cube callers may pass <em>SQL expressions</em> (rather than plain column names) as the column of an aggregator or
 * group-by — the most common shapes:
 * <ul>
 * <li>{@code approx_count_distinct(c_custkey)} — DuckDB's cardinality estimate over a join column.</li>
 * <li>{@code sum(l_extendedprice * (1 - l_discount))} — TPC-H {@code revenue} over multiple base columns.</li>
 * <li>{@code SUBSTRING("a_name", 1, 1)} — per-letter facet over a join column.</li>
 * </ul>
 * Strict-mode {@link PrunedJoinsJooqTableSupplier#computeNeededAliases(Set)} would otherwise reject these — none of the
 * strings is a plain column. The supplier first hands the unknown reference to an {@link IExpressionColumnExtractor};
 * the returned column names go through the regular index lookup, pulling in any join the expression touches.
 * <p>
 * Default impl: {@link KnownColumnsExpressionExtractor} (token-boundary substring match against the supplier's
 * column→alias index). Projects with rich SQL parsing needs can plug in a JOOQ-parser-based extractor by passing their
 * own implementation to {@code PrunedJoinsJooqTableSupplier.builder().expressionColumnExtractor(...)}.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IExpressionColumnExtractor {

	/**
	 * @param expression
	 *            the candidate "column" string from the cube query — may be a plain column name, an SQL expression, or
	 *            anything else the cube author put there.
	 * @param knownColumns
	 *            the supplier's column→alias index keys — the set of column names the supplier knows about. Useful for
	 *            substring matching strategies.
	 * @return the set of underlying column names referenced by {@code expression}. Empty when no underlying column can
	 *         be deduced (the supplier will then surface a strict-mode error with the original expression).
	 */
	Set<String> extractColumns(String expression, Set<String> knownColumns);
}
