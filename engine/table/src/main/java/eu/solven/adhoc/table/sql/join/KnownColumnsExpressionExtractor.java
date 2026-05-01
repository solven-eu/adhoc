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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Default {@link IExpressionColumnExtractor} — extracts column references from an SQL expression by scanning the
 * known-columns set for token-boundary substring matches.
 * <p>
 * Why this approach rather than a full SQL parser:
 * <ul>
 * <li><b>Dialect-agnostic.</b> The supplier serves DuckDB, Redshift, BigQuery, … Each has its own SQL surface; a single
 * parser would need to cover them all.</li>
 * <li><b>Cheap.</b> A linear scan over the index keys (typically a few dozen) is orders of magnitude faster than
 * parsing — and {@code computeNeededAliases} runs once per distinct column-set per cache miss.</li>
 * <li><b>Good enough for the bug class.</b> The expressions that triggered the strict-mode failures
 * ({@code approx_count_distinct(c_custkey)}, {@code sum(l_extendedprice * (1 - l_discount))},
 * {@code SUBSTRING("a_name", 1, 1)}) all embed the underlying column names verbatim.</li>
 * </ul>
 * Token boundaries are detected via {@link Character#isJavaIdentifierPart(char)}: a column name is matched only when
 * adjacent characters are NOT identifier characters (or the string boundary). That avoids false positives like
 * "{@code a_name}" appearing inside "{@code a_name_extra}".
 * <p>
 * Projects whose expressions reference columns through aliases not in the index, or that need to disambiguate via full
 * SQL parsing, can plug in their own {@link IExpressionColumnExtractor} via
 * {@code PrunedJoinsJooqTableSupplier.builder().expressionColumnExtractor(...)}.
 *
 * @author Benoit Lacelle
 */
public final class KnownColumnsExpressionExtractor implements IExpressionColumnExtractor {

	public static final KnownColumnsExpressionExtractor INSTANCE = new KnownColumnsExpressionExtractor();

	// Quoted column name (double-quote, backtick, or square-bracket) — checked verbatim before trying token-boundary
	// matching. JOOQ's `Name.toString()` produces double-quoted segments, so the index can hold entries like
	// `"customer"."c_custkey"` whose internal punctuation would defeat the token-boundary heuristic.
	private static final Pattern SIMPLE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

	private KnownColumnsExpressionExtractor() {
	}

	@Override
	public Set<String> extractColumns(String expression, Set<String> knownColumns) {
		if (expression == null || expression.isEmpty() || knownColumns.isEmpty()) {
			return Set.of();
		}
		Set<String> found = new LinkedHashSet<>();
		for (String col : knownColumns) {
			if (col.isEmpty()) {
				continue;
			}
			if (SIMPLE_IDENTIFIER.matcher(col).matches()) {
				// Identifier-shaped: enforce token boundaries so `a_name` doesn't match inside `a_name_extra`.
				if (containsAsToken(expression, col)) {
					found.add(col);
				}
			} else if (expression.contains(col)) {
				// Already-quoted or otherwise punctuation-rich form: a plain substring check is sufficient (and
				// safer — no risk of partial-token false positives because the column name itself contains
				// non-identifier characters that the surrounding expression cannot extend through).
				found.add(col);
			}
		}
		return found;
	}

	/**
	 * @return {@code true} iff {@code needle} appears in {@code haystack} bounded by non-identifier characters (or
	 *         string boundaries) on both sides — e.g. matches "a_name" in "approx_count_distinct(a_name)" but not in
	 *         "a_name_extra".
	 */
	private static boolean containsAsToken(String haystack, String needle) {
		int from = 0;
		while (true) {
			int idx = haystack.indexOf(needle, from);
			if (idx < 0) {
				return false;
			}
			boolean leftOk = idx == 0 || !Character.isJavaIdentifierPart(haystack.charAt(idx - 1));
			int end = idx + needle.length();
			boolean rightOk = end == haystack.length() || !Character.isJavaIdentifierPart(haystack.charAt(end));
			if (leftOk && rightOk) {
				return true;
			}
			from = idx + 1;
		}
	}
}
